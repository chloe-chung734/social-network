"""Small HTTP client for the Chord peer API (chord_peer.py, chord_contract.md).

Post uses it to find a record's owner through one entry peer (/lookup), then
reads or writes the record on that owner (/records/get, /records/put,
/records/delete). Set CHORD_ENTRY_PEER to choose the entry peer.
"""

import hashlib
import os
import time
from urllib.parse import urljoin

import requests


RING_SIZE = 32
DEFAULT_ENTRY_PEER = "http://127.0.0.1:7004"


class ChordError(Exception):
    """Base error for peer API failures."""


class ChordUnavailable(ChordError):
    """No peer could be reached or peer storage reported an error."""


class ChordTimeout(ChordError):
    """The shared operation deadline expired."""


class ChordClient:
    """Route each record operation through an entry peer and its lookup API."""

    def __init__(self, entry_peer=None, timeout=2.0):
        self.entry_peer = (entry_peer or os.getenv(
            "CHORD_ENTRY_PEER", DEFAULT_ENTRY_PEER)).rstrip("/")
        self.timeout = float(timeout)
        self.session = requests.Session()

    @staticmethod
    def routing_key(post_id):
        """Return SHA-256(decimal post ID) modulo the 32-position ring."""
        digest = hashlib.sha256(str(post_id).encode("ascii")).digest()
        return int.from_bytes(digest, "big") % RING_SIZE

    def _post(self, base, path, body, request_id, deadline=None):
        remaining = self.timeout if deadline is None else deadline - time.monotonic()
        if remaining <= 0:
            raise ChordTimeout("Chord operation deadline expired")
        try:
            response = self.session.post(
                urljoin(base.rstrip("/") + "/", path.lstrip("/")),
                json=body,
                headers={"X-Request-ID": request_id},
                timeout=max(0.05, min(self.timeout, remaining)),
            )
        except requests.Timeout as exc:
            raise ChordTimeout(str(exc)) from exc
        except requests.RequestException as exc:
            raise ChordUnavailable(str(exc)) from exc
        if response.status_code == 404 and path == "/records/get":
            return {"success": True, "found": False}
        if response.status_code >= 500:
            raise ChordUnavailable(
                "peer returned HTTP %s: %s" % (response.status_code, response.text))
        try:
            result = response.json()
        except ValueError as exc:
            raise ChordUnavailable("peer returned invalid JSON") from exc
        if not response.ok or not isinstance(result, dict) or result.get("success") is not True:
            raise ChordUnavailable("peer rejected request: %s" % result)
        return result

    def lookup(self, key, request_id, deadline=None):
        if type(key) is not int or not 0 <= key < RING_SIZE:
            raise ValueError("routing key must be an integer in [0, 31]")
        result = self._post(self.entry_peer, "/lookup", {
            "key": key,
        }, request_id, deadline)
        owner = result.get("owner")
        if not isinstance(owner, dict) or not isinstance(owner.get("url"), str):
            raise ChordUnavailable("lookup response has no owner.url")
        return owner

    def get(self, record_id, routing_key, request_id, deadline=None):
        owner = self.lookup(routing_key, request_id, deadline)
        result = self._post(owner["url"], "/records/get", {
            "record_id": str(record_id), "routing_key": routing_key,
        }, request_id, deadline)
        if result.get("found") is not True:
            return None
        if "record" not in result:
            raise ChordUnavailable("record response has no record")
        return result["record"]

    def put(self, record_id, routing_key, value, request_id, deadline=None):
        owner = self.lookup(routing_key, request_id, deadline)
        return self._post(owner["url"], "/records/put", {
            "record_id": str(record_id), "routing_key": routing_key,
            "value": value, "operation_id": request_id,
        }, request_id, deadline)

    def delete(self, record_id, routing_key, request_id, deadline=None):
        owner = self.lookup(routing_key, request_id, deadline)
        return self._post(owner["url"], "/records/delete", {
            "record_id": str(record_id), "routing_key": routing_key,
            "operation_id": request_id,
        }, request_id, deadline)
