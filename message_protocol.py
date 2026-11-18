"""Message metadata and clock logging shared by every process (Milestone 4).

One MessageContext per process. It owns one boot UUID, one VectorClock and
one JSONL coordination/clock log. Create it once at startup and share it
with every helper client in the process (never create a second one):

    ctx = MessageContext(os.getenv("NODE_NAME", "post-a"))
    # ctx.node_id == "post-a:<boot-uuid>"

WHERE TO CALL THE HELPERS (the rules that make the clocks correct)
    * send_headers / send_payload: once per message actually transmitted,
      immediately before sending it. Each call is one send event. A retry is
      a NEW send even with the same request ID, so call it again per attempt.
      Never cache the returned headers or reuse them for a second message.
    * receive_headers / receive_payload: once per message received, as soon
      as it arrives and before application logic that depends on it. Each
      call is one receive event; do not call it twice for the same message.
    * An HTTP server's response is a send: call send_headers when building
      every response (including error responses).
    * A response you receive is a receive: call receive_headers(resp.headers)
      when one arrives. If no response arrives (timeout), there is no
      receive event.
    * A broker relaying an event does receive_payload (or receive_headers)
      on the incoming event, then a separate send for EACH forwarded copy,
      passing the application payload that receive_payload returned.
    * Application events that are not messages (CS_ENTER, CS_EXIT, ...) call
      ctx.local_event(...), which ticks the clock then logs. log() alone
      never advances the clock.

MALFORMED vs UNTRACKED
    Input with NO X-Node-ID / X-Vector-Clock (or no "_meta") is legacy input:
    it is accepted, the clock does NOT change, and an UNTRACKED_INPUT line is
    logged. No causal claim covers it. Metadata that is present but partial or
    invalid raises MessageProtocolError and does not change the clock; the
    caller should answer HTTP 400 (or reject the TCP message).

USAGE: Flask handler (server side)
    from flask import g, jsonify, request
    ctx = MessageContext("post-a")

    @app.before_request
    def clock_receive():
        try:
            g.request_id = ctx.receive_headers(request.headers)
        except MessageProtocolError as exc:
            g.request_id = str(uuid.uuid4())
            return jsonify({"success": False, "error": str(exc)}), 400

    @app.after_request
    def clock_send(response):
        response.headers.update(ctx.send_headers(g.request_id))
        return response

    (after_request also runs for the 400 above, so g.request_id is set first.)

USAGE: requests call (client side)
    request_id = str(uuid.uuid4())              # once per operation
    for attempt in range(3):
        headers = ctx.send_headers(request_id)  # new send event every attempt
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=2)
        except requests.RequestException:
            continue                            # no response, no receive event
        ctx.receive_headers(resp.headers)       # response arrived: one receive
        break

USAGE: TCP JSON messages (newline-terminated)
    # sender
    message = ctx.send_payload({"type": "demo_status_request"}, request_id)
    sock.sendall(json.dumps(message).encode() + b"\\n")
    # receiver
    payload, request_id = ctx.receive_payload(json.loads(line))
    reply = ctx.send_payload({"success": True}, request_id)  # the ack is a send

Header names are case-insensitive on input. The application JSON body is
never changed; TCP messages carry the metadata under "_meta".
"""

import copy
import json
import logging
import os
import re
import threading
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone

from vector_clock import (VectorClock, VectorClockError, decode_vector,
                          encode_vector, validate_vector)

HEADER_NODE_ID = "X-Node-ID"
HEADER_REQUEST_ID = "X-Request-ID"
HEADER_VECTOR_CLOCK = "X-Vector-Clock"
META_KEY = "_meta"

_PROTOCOL_HEADERS = {HEADER_NODE_ID.lower(), HEADER_REQUEST_ID.lower(),
                     HEADER_VECTOR_CLOCK.lower()}
_NODE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_TOKEN = re.compile(r"[\x21-\x7e]{1,300}")           # printable ASCII, no spaces
_REQUEST_ID = re.compile(r"[\x21-\x7e]{1,256}")
_RESERVED_LOG_FIELDS = {"node_id", "request_id", "event", "vector_clock",
                        "wall_time"}

_warn = logging.getLogger("message_protocol")


class MessageProtocolError(ValueError):
    """Message metadata is present but malformed, or an argument is invalid."""


def _valid_request_id(value):
    return isinstance(value, str) and _REQUEST_ID.fullmatch(value) is not None


def _check_request_id(value):
    if not _valid_request_id(value):
        raise MessageProtocolError(
            "request_id must be 1-256 printable ASCII characters without spaces")
    return value


class MessageContext:
    """The per-process identity, clock and log used by all message helpers."""

    def __init__(self, node_name, *, boot_id=None, log_path=None):
        if not isinstance(node_name, str) or _NODE_NAME.fullmatch(node_name) is None:
            raise MessageProtocolError(
                "node_name must be 1-64 characters of letters, digits, '.', '_' "
                "or '-', starting with a letter or digit")
        if boot_id is None:
            boot_id = str(uuid.uuid4())
        elif not isinstance(boot_id, str) or _TOKEN.fullmatch(boot_id) is None:
            raise MessageProtocolError("boot_id must be printable ASCII")
        self.node_name = node_name
        self.boot_id = boot_id
        self.node_id = "%s:%s" % (node_name, boot_id)
        self.clock = VectorClock(self.node_id)
        self.log_path = log_path or os.path.join(
            os.getenv("CLOCK_LOG_DIR", "logs"), "%s.clock.jsonl" % node_name)
        self._log_lock = threading.Lock()

    # ------------------------------------------------------------- HTTP

    def send_headers(self, request_id, extra=None):
        """Advance the clock once; return headers for ONE outgoing message.

        Supplied extra headers are preserved. Any extra header named like a
        protocol header (any letter case) is replaced by the generated one.
        """
        _check_request_id(request_id)
        if extra is not None and not isinstance(extra, Mapping):
            raise MessageProtocolError("extra headers must be a mapping")
        headers = {key: value for key, value in (extra or {}).items()
                   if str(key).lower() not in _PROTOCOL_HEADERS}
        vector = self.clock.send()
        headers[HEADER_NODE_ID] = self.node_id
        headers[HEADER_REQUEST_ID] = request_id
        headers[HEADER_VECTOR_CLOCK] = encode_vector(vector)
        return headers

    def receive_headers(self, headers, *, default_request_id=None):
        """Merge the sender's vector (one receive event); return the request ID.

        No X-Node-ID and no X-Vector-Clock: untracked legacy input. The clock
        is not changed, UNTRACKED_INPUT is logged, and the X-Request-ID is
        returned (or default_request_id, or a new UUID).
        Present but partial/invalid metadata raises MessageProtocolError.
        """
        lowered = self._lower_headers(headers)
        node_id = lowered.get("x-node-id")
        vector_text = lowered.get("x-vector-clock")
        supplied_rid = lowered.get("x-request-id")
        if node_id is None and vector_text is None:
            return self._untracked(supplied_rid, default_request_id, "http")
        if node_id is None or vector_text is None:
            raise MessageProtocolError(
                "X-Node-ID and X-Vector-Clock must be supplied together")
        if not isinstance(node_id, str) or _TOKEN.fullmatch(node_id) is None:
            raise MessageProtocolError("X-Node-ID is malformed")
        if not _valid_request_id(supplied_rid):
            raise MessageProtocolError(
                "X-Request-ID is required and must be well formed on tracked input")
        try:
            vector = decode_vector(vector_text)
            self.clock.receive(vector)
        except VectorClockError as exc:
            raise MessageProtocolError("X-Vector-Clock is invalid: %s" % exc) from exc
        return supplied_rid

    @staticmethod
    def _lower_headers(headers):
        if headers is None:
            return {}
        if not hasattr(headers, "items"):
            raise MessageProtocolError("headers must be a mapping")
        lowered = {}
        for key, value in headers.items():
            name = str(key).lower()
            if name in _PROTOCOL_HEADERS and name in lowered and lowered[name] != value:
                raise MessageProtocolError("conflicting duplicate %s headers" % key)
            lowered[name] = value
        return lowered

    # ------------------------------------------------- TCP / JSON bodies

    def send_payload(self, payload, request_id):
        """Advance the clock once; return a COPY of payload plus "_meta"."""
        if not isinstance(payload, dict):
            raise MessageProtocolError("payload must be a JSON object (dict)")
        if META_KEY in payload:
            raise MessageProtocolError(
                "payload already contains %r; pass the application payload "
                "returned by receive_payload" % META_KEY)
        _check_request_id(request_id)
        message = copy.deepcopy(payload)
        vector = self.clock.send()
        message[META_KEY] = {"node_id": self.node_id, "request_id": request_id,
                             "vector_clock": vector}
        return message

    def receive_payload(self, payload, *, default_request_id=None):
        """Merge the sender's vector; return (application_payload, request_id).

        The caller's payload is not mutated; the returned application payload
        is a copy without "_meta". A payload with no "_meta" is untracked.
        """
        if not isinstance(payload, dict):
            raise MessageProtocolError("message must be a JSON object (dict)")
        if META_KEY not in payload:
            request_id = self._untracked(None, default_request_id, "payload")
            return copy.deepcopy(payload), request_id
        meta = payload[META_KEY]
        if not isinstance(meta, dict):
            raise MessageProtocolError("%s must be an object" % META_KEY)
        for field in ("node_id", "request_id", "vector_clock"):
            if field not in meta:
                raise MessageProtocolError("%s.%s is missing" % (META_KEY, field))
        if not isinstance(meta["node_id"], str) or _TOKEN.fullmatch(meta["node_id"]) is None:
            raise MessageProtocolError("%s.node_id is malformed" % META_KEY)
        request_id = _check_request_id(meta["request_id"])
        try:
            vector = validate_vector(meta["vector_clock"])
        except VectorClockError as exc:
            raise MessageProtocolError("%s.vector_clock is invalid: %s"
                                       % (META_KEY, exc)) from exc
        application = {key: copy.deepcopy(value) for key, value in payload.items()
                       if key != META_KEY}
        try:
            self.clock.receive(vector)
        except VectorClockError as exc:
            raise MessageProtocolError("cannot merge vector clock: %s" % exc) from exc
        return application, request_id

    # --------------------------------------------------------- logging

    def log(self, event, request_id, **fields):
        """Append one JSONL record with the current clock. Does NOT advance it.

        Under concurrency the snapshot may already include events from other
        threads; use local_event() when the record must show the exact vector
        produced by an application event.
        """
        return self._write(event, request_id, self.clock.snapshot(), fields)

    def local_event(self, event, request_id, **fields):
        """Application event (CS_ENTER, CS_EXIT, ...): tick, then log that vector."""
        return self._write(event, request_id, self.clock.tick(), fields)

    def _write(self, event, request_id, vector, fields):
        if not isinstance(event, str) or not event:
            raise MessageProtocolError("event must be a nonempty string")
        _check_request_id(request_id)
        clash = _RESERVED_LOG_FIELDS & fields.keys()
        if clash:
            raise MessageProtocolError("reserved log fields: %s" % sorted(clash))
        record = {"node_id": self.node_id, "request_id": request_id,
                  "event": event, "vector_clock": vector,
                  "wall_time": datetime.now(timezone.utc).isoformat()}
        record.update(fields)
        line = json.dumps(record, separators=(",", ":"), default=str,
                          allow_nan=False) + "\n"
        with self._log_lock:
            try:
                directory = os.path.dirname(self.log_path)
                if directory:
                    os.makedirs(directory, exist_ok=True)
                with open(self.log_path, "a", encoding="utf-8") as handle:
                    handle.write(line)
            except OSError as exc:
                # Logging must not crash the protocol, but must not be silent.
                _warn.warning("clock log write failed (%s): %s", self.log_path, exc)
        return record

    def _untracked(self, supplied_rid, default_request_id, transport):
        if default_request_id is not None:
            _check_request_id(default_request_id)
        generated = False
        invalid = supplied_rid is not None and not _valid_request_id(supplied_rid)
        if _valid_request_id(supplied_rid):
            request_id = supplied_rid
        elif default_request_id is not None:
            request_id, generated = default_request_id, True
        else:
            request_id, generated = str(uuid.uuid4()), True
        self.log("UNTRACKED_INPUT", request_id, transport=transport,
                 generated_request_id=generated, supplied_request_id_invalid=invalid)
        return request_id
