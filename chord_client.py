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


class ChordOutcomeUnknown(ChordError):
    """A mutation may have reached storage, but its result was not confirmed."""


class ChordClient:
    """Route each record operation through an entry peer and its lookup API."""

    def __init__(self, entry_peer=None, timeout=2.0, message_context=None):
        self.entry_peer = (entry_peer or os.getenv(
            "CHORD_ENTRY_PEER", DEFAULT_ENTRY_PEER)).rstrip("/")
        self.timeout = float(timeout)
        self.session = requests.Session()
        self.message_context = message_context

    @staticmethod
    def routing_key(post_id):
        """Return SHA-256(decimal post ID) modulo the 32-position ring."""
        digest = hashlib.sha256(str(post_id).encode("ascii")).digest()
        return int.from_bytes(digest, "big") % RING_SIZE

    def _post(self, base, path, body, request_id, deadline=None, *, mutation=False):
        remaining = self.timeout if deadline is None else deadline - time.monotonic()
        if remaining <= 0:
            raise ChordTimeout("Chord operation deadline expired")
        headers = (self.message_context.send_headers(request_id)
                   if self.message_context is not None
                   else {"X-Request-ID": request_id})
        try:
            response = self.session.post(
                urljoin(base.rstrip("/") + "/", path.lstrip("/")),
                json=body,
                headers=headers,
                timeout=max(0.05, min(self.timeout, remaining)),
            )
        except requests.Timeout as exc:
            if mutation:
                raise ChordOutcomeUnknown(str(exc)) from exc
            raise ChordTimeout(str(exc)) from exc
        except requests.RequestException as exc:
            if mutation:
                raise ChordOutcomeUnknown(str(exc)) from exc
            raise ChordUnavailable(str(exc)) from exc
        if self.message_context is not None:
            try:
                response_request_id = self.message_context.receive_headers(response.headers)
            except Exception as exc:
                if mutation:
                    raise ChordOutcomeUnknown(
                        "mutation reply metadata invalid; outcome is unknown") from exc
                raise ChordUnavailable("peer returned invalid message metadata: %s" % exc) from exc
            if response_request_id != request_id:
                if mutation:
                    raise ChordOutcomeUnknown(
                        "mutation reply correlation ID did not match request")
                raise ChordUnavailable("peer reply correlation ID did not match request")
        if response.status_code == 404 and path == "/records/get":
            return {"success": True, "found": False}
        if response.status_code >= 500:
            if mutation:
                raise ChordOutcomeUnknown(
                    "peer returned HTTP %s: %s" % (response.status_code, response.text))
            raise ChordUnavailable(
                "peer returned HTTP %s: %s" % (response.status_code, response.text))
        try:
            result = response.json()
        except ValueError as exc:
            if mutation and response.ok:
                raise ChordOutcomeUnknown(
                    "peer accepted mutation but returned invalid JSON") from exc
            raise ChordUnavailable("peer returned invalid JSON") from exc
        if not response.ok or not isinstance(result, dict) or result.get("success") is not True:
            if mutation and response.ok:
                raise ChordOutcomeUnknown(
                    "peer accepted mutation but returned an invalid success reply")
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

    def put(self, record_id, routing_key, value, request_id, deadline=None,
            coordination=None):
        owner = self.lookup(routing_key, request_id, deadline)
        body = {
            "record_id": str(record_id), "routing_key": routing_key,
            "value": value, "operation_id": request_id,
        }
        if coordination is not None:
            body["coordination"] = coordination
        return self._post(owner["url"], "/records/put", body,
                          request_id, deadline, mutation=True)

    def delete(self, record_id, routing_key, request_id, deadline=None,
               coordination=None):
        owner = self.lookup(routing_key, request_id, deadline)
        body = {
            "record_id": str(record_id), "routing_key": routing_key,
            "operation_id": request_id,
        }
        if coordination is not None:
            body["coordination"] = coordination
        return self._post(owner["url"], "/records/delete", body,
                          request_id, deadline, mutation=True)
