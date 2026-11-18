import copy
import json
import os
import tempfile
import unittest

from message_protocol import (HEADER_NODE_ID, HEADER_REQUEST_ID,
                              HEADER_VECTOR_CLOCK, META_KEY, MessageContext,
                              MessageProtocolError)
from vector_clock import compare_vectors, decode_vector


class Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name

    def make(self, name):
        return MessageContext(name, log_path=os.path.join(self.dir, name + ".jsonl"))

    def records(self, ctx):
        if not os.path.exists(ctx.log_path):
            return []
        with open(ctx.log_path, encoding="utf-8") as handle:
            return [json.loads(line) for line in handle]

    def own(self, ctx):
        return ctx.clock.snapshot()[ctx.node_id]


class Identity(Base):
    def test_node_id_has_name_and_boot_uuid_and_each_context_is_unique(self):
        a, b = self.make("post-a"), self.make("post-a")
        self.assertTrue(a.node_id.startswith("post-a:"))
        self.assertEqual(a.node_id, "post-a:" + a.boot_id)
        self.assertNotEqual(a.node_id, b.node_id)
        self.assertEqual(a.clock.node_id, a.node_id)

    def test_bad_node_names_rejected(self):
        for bad in ("", "has space", "a:b", "-x", None, 3, "x" * 65, "a\r\nb"):
            with self.assertRaises(MessageProtocolError):
                MessageContext(bad)


class Headers(Base):
    def test_send_headers_advances_exactly_once_and_carries_metadata(self):
        a = self.make("a")
        headers = a.send_headers("req-1")
        self.assertEqual(self.own(a), 1)
        self.assertEqual(headers[HEADER_NODE_ID], a.node_id)
        self.assertEqual(headers[HEADER_REQUEST_ID], "req-1")
        self.assertEqual(decode_vector(headers[HEADER_VECTOR_CLOCK]),
                         a.clock.snapshot())
        a.send_headers("req-1")                       # a retry is a new send
        self.assertEqual(self.own(a), 2)

    def test_extra_headers_preserved_and_not_mutated(self):
        a = self.make("a")
        extra = {"Content-Type": "application/json", "x-request-id": "old",
                 "X-VECTOR-CLOCK": "stale"}
        snapshot = dict(extra)
        headers = a.send_headers("req-1", extra)
        self.assertEqual(extra, snapshot)
        self.assertEqual(headers["Content-Type"], "application/json")
        lowered = [k.lower() for k in headers]
        self.assertEqual(lowered.count("x-request-id"), 1)
        self.assertEqual(lowered.count("x-vector-clock"), 1)
        self.assertEqual(headers[HEADER_REQUEST_ID], "req-1")

    def test_receive_headers_merges_and_advances_once_case_insensitive(self):
        a, b = self.make("a"), self.make("b")
        sent = a.send_headers("req-9")
        lowered = {k.lower(): v for k, v in sent.items()}
        self.assertEqual(b.receive_headers(lowered), "req-9")
        self.assertEqual(self.own(b), 1)
        self.assertEqual(b.clock.snapshot()[a.node_id], 1)
        shouty = {k.upper(): v for k, v in sent.items()}
        b.receive_headers(shouty)
        self.assertEqual(self.own(b), 2)

    def test_round_trip_request_and_response_are_causally_ordered(self):
        client, server = self.make("client"), self.make("post-a")
        request = client.send_headers("op-1")
        server.receive_headers(request)
        after_receive = server.clock.snapshot()
        response = server.send_headers("op-1")
        client.receive_headers(response)
        sent = decode_vector(request[HEADER_VECTOR_CLOCK])
        replied = decode_vector(response[HEADER_VECTOR_CLOCK])
        self.assertEqual(compare_vectors(sent, after_receive), "before")
        self.assertEqual(compare_vectors(after_receive, replied), "before")
        self.assertEqual(compare_vectors(replied, client.clock.snapshot()), "before")

    def test_receive_headers_accepts_mapping_like_objects(self):
        a, b = self.make("a"), self.make("b")
        sent = a.send_headers("r")

        class Wrapper:                      # like werkzeug / requests header objects
            def __init__(self, data): self._d = data
            def items(self): return self._d.items()

        self.assertEqual(b.receive_headers(Wrapper(sent)), "r")
        try:
            from werkzeug.datastructures import Headers
            self.assertEqual(self.make("c").receive_headers(Headers(list(sent.items()))), "r")
        except ImportError:
            pass
        try:
            from requests.structures import CaseInsensitiveDict
            self.assertEqual(self.make("d").receive_headers(CaseInsensitiveDict(sent)), "r")
        except ImportError:
            pass

    def test_untracked_legacy_input_does_not_touch_clock_and_is_logged(self):
        b = self.make("b")
        before = b.clock.snapshot()
        self.assertEqual(b.receive_headers({"X-Request-ID": "legacy-1"}), "legacy-1")
        generated = b.receive_headers({})
        self.assertEqual(len(generated), 36)
        self.assertEqual(b.receive_headers(None, default_request_id="mine"), "mine")
        self.assertEqual(b.clock.snapshot(), before)
        events = self.records(b)
        self.assertEqual([e["event"] for e in events], ["UNTRACKED_INPUT"] * 3)
        self.assertEqual(events[0]["request_id"], "legacy-1")
        self.assertFalse(events[0]["generated_request_id"])
        self.assertTrue(events[1]["generated_request_id"])
        self.assertEqual(events[0]["vector_clock"], before)

    def test_untracked_invalid_request_id_gets_replaced(self):
        b = self.make("b")
        rid = b.receive_headers({"X-Request-ID": "has space"})
        self.assertNotEqual(rid, "has space")
        self.assertTrue(self.records(b)[0]["supplied_request_id_invalid"])

    def test_malformed_tracked_headers_raise_and_leave_clock_unchanged(self):
        a, b = self.make("a"), self.make("b")
        good = a.send_headers("r")
        before = b.clock.snapshot()
        cases = {
            "vector only": {k: v for k, v in good.items() if k != HEADER_NODE_ID},
            "node only": {k: v for k, v in good.items() if k != HEADER_VECTOR_CLOCK},
            "no request id": {k: v for k, v in good.items() if k != HEADER_REQUEST_ID},
            "bad json": dict(good, **{HEADER_VECTOR_CLOCK: "{nope"}),
            "float counter": dict(good, **{HEADER_VECTOR_CLOCK: '{"a":1.5}'}),
            "negative": dict(good, **{HEADER_VECTOR_CLOCK: '{"a":-1}'}),
            "array": dict(good, **{HEADER_VECTOR_CLOCK: "[1]"}),
            "bad node id": dict(good, **{HEADER_NODE_ID: "has space"}),
            "bad request id": dict(good, **{HEADER_REQUEST_ID: "bad id"}),
            "conflicting duplicates": [("X-Vector-Clock", '{"a":1}'),
                                       ("x-vector-clock", '{"a":2}'),
                                       ("X-Node-ID", "n"), ("X-Request-ID", "r")],
            "not a mapping": 5,
        }
        for name, headers in cases.items():
            with self.subTest(name):
                if isinstance(headers, list):
                    headers = _Pairs(headers)
                with self.assertRaises(MessageProtocolError):
                    b.receive_headers(headers)
        self.assertEqual(b.clock.snapshot(), before)

    def test_send_headers_rejects_bad_request_id_without_advancing(self):
        a = self.make("a")
        for bad in ("", None, 5, "a b", "x\r\ny", "z" * 257):
            with self.assertRaises(MessageProtocolError):
                a.send_headers(bad)
        with self.assertRaises(MessageProtocolError):
            a.send_headers("ok", extra=[("k", "v")])
        self.assertEqual(self.own(a), 0)


class _Pairs:
    def __init__(self, pairs): self._pairs = pairs
    def items(self): return list(self._pairs)


class Payloads(Base):
    def test_round_trip_preserves_application_payload(self):
        a, b = self.make("a"), self.make("b")
        original = {"type": "demo", "n": [1, {"x": 2}], "message_id": "m1"}
        keep = copy.deepcopy(original)
        wire = json.loads(json.dumps(a.send_payload(original, "req-1")))
        self.assertEqual(original, keep)                  # caller payload untouched
        self.assertEqual(self.own(a), 1)
        meta = wire[META_KEY]
        self.assertEqual(meta["node_id"], a.node_id)
        self.assertEqual(meta["request_id"], "req-1")
        self.assertEqual(meta["vector_clock"], a.clock.snapshot())
        received_copy = copy.deepcopy(wire)
        app, rid = b.receive_payload(wire)
        self.assertEqual(wire, received_copy)             # received dict untouched
        self.assertEqual((app, rid), (original, "req-1"))
        self.assertNotIn(META_KEY, app)
        self.assertEqual(self.own(b), 1)
        app["n"][1]["x"] = 99                              # result is a copy
        self.assertEqual(wire["n"][1]["x"], 2)

    def test_send_payload_returns_independent_copy(self):
        a = self.make("a")
        original = {"nested": {"k": 1}}
        message = a.send_payload(original, "r")
        message["nested"]["k"] = 2
        self.assertEqual(original["nested"]["k"], 1)

    def test_broker_forwarding_does_its_own_receive_and_send(self):
        publisher, broker, sub = self.make("post-a"), self.make("broker"), self.make("feed")
        event = {"event_id": "e1", "type": "post.created", "post": {"id": 1}}
        incoming = publisher.send_payload(event, "op-1")
        app, rid = broker.receive_payload(incoming)
        self.assertEqual(self.own(broker), 1)
        out1 = broker.send_payload(app, rid)
        out2 = broker.send_payload(app, rid)              # one send per recipient
        self.assertEqual(self.own(broker), 3)
        self.assertEqual(out1["event_id"], "e1")
        self.assertEqual(out1["post"], {"id": 1})
        self.assertEqual(compare_vectors(incoming[META_KEY]["vector_clock"],
                                         out1[META_KEY]["vector_clock"]), "before")
        self.assertEqual(compare_vectors(out1[META_KEY]["vector_clock"],
                                         out2[META_KEY]["vector_clock"]), "before")
        sub.receive_payload(out1)
        self.assertEqual(sub.clock.snapshot()[publisher.node_id], 1)

    def test_payload_without_meta_is_untracked(self):
        b = self.make("b")
        before = b.clock.snapshot()
        app, rid = b.receive_payload({"type": "demo_status_request", "message_id": "m"})
        self.assertEqual(app, {"type": "demo_status_request", "message_id": "m"})
        self.assertEqual(len(rid), 36)
        self.assertEqual(b.clock.snapshot(), before)
        self.assertEqual(self.records(b)[0]["event"], "UNTRACKED_INPUT")
        self.assertEqual(self.records(b)[0]["transport"], "payload")

    def test_malformed_meta_rejected_and_clock_unchanged(self):
        a, b = self.make("a"), self.make("b")
        good = a.send_payload({"x": 1}, "r")[META_KEY]
        before = b.clock.snapshot()
        bad_metas = [None, [], "x", {}, {"node_id": "n", "request_id": "r"},
                     dict(good, vector_clock=[]), dict(good, vector_clock={"a": -1}),
                     dict(good, vector_clock={"a": 1.5}), dict(good, vector_clock={"a": True}),
                     dict(good, vector_clock="{}"), dict(good, request_id=""),
                     dict(good, request_id=7), dict(good, node_id=""),
                     dict(good, node_id="has space")]
        for meta in bad_metas:
            with self.subTest(meta=meta):
                with self.assertRaises(MessageProtocolError):
                    b.receive_payload({"x": 1, META_KEY: meta})
        with self.assertRaises(MessageProtocolError):
            b.receive_payload([1, 2])
        self.assertEqual(b.clock.snapshot(), before)

    def test_send_payload_rejects_bad_input_without_advancing(self):
        a = self.make("a")
        for payload, rid in (([], "r"), ({"a": 1}, ""), ({META_KEY: {}}, "r")):
            with self.assertRaises(MessageProtocolError):
                a.send_payload(payload, rid)
        self.assertEqual(self.own(a), 0)


class Logging(Base):
    def test_log_records_snapshot_without_advancing(self):
        a = self.make("a")
        a.send_headers("r1")
        before = a.clock.snapshot()
        record = a.log("LOCK_REQUEST", "r1", resource="post-write", grant_token=None)
        self.assertEqual(a.clock.snapshot(), before)
        lines = self.records(a)
        self.assertEqual(len(lines), 1)
        line = lines[0]
        for field in ("node_id", "request_id", "event", "vector_clock"):
            self.assertIn(field, line)
        self.assertEqual(line["vector_clock"], before)
        self.assertEqual(line["resource"], "post-write")
        self.assertEqual(line["node_id"], a.node_id)
        self.assertEqual(record["event"], "LOCK_REQUEST")

    def test_log_snapshot_is_not_aliased_to_the_clock(self):
        a = self.make("a")
        record = a.log("E", "r")
        a.clock.tick()
        self.assertEqual(record["vector_clock"], {a.node_id: 0})

    def test_local_event_ticks_then_logs_exact_vector(self):
        a = self.make("a")
        record = a.local_event("CS_ENTER", "r", resource="post-write")
        self.assertEqual(self.own(a), 1)
        self.assertEqual(record["vector_clock"], {a.node_id: 1})
        a.local_event("CS_EXIT", "r")
        events = self.records(a)
        self.assertEqual([e["event"] for e in events], ["CS_ENTER", "CS_EXIT"])
        self.assertEqual(compare_vectors(events[0]["vector_clock"],
                                         events[1]["vector_clock"]), "before")

    def test_reserved_and_invalid_arguments_rejected(self):
        a = self.make("a")
        for kwargs in ({"vector_clock": {}}, {"node_id": "x"}, {"wall_time": 1}):
            with self.assertRaises(MessageProtocolError):
                a.log("E", "r", **kwargs)
        with self.assertRaises(MessageProtocolError):
            a.log("", "r")
        with self.assertRaises(MessageProtocolError):
            a.log("E", "bad id")
        self.assertEqual(self.records(a), [])

    def test_unserializable_field_values_are_stringified(self):
        a = self.make("a")
        a.log("E", "r", obj=object)
        self.assertIsInstance(self.records(a)[0]["obj"], str)

    def test_unwritable_log_path_does_not_raise(self):
        blocker = os.path.join(self.dir, "file")
        open(blocker, "w").close()
        ctx = MessageContext("a", log_path=os.path.join(blocker, "sub", "x.jsonl"))
        with self.assertLogs("message_protocol", level="WARNING"):
            ctx.log("E", "r")

    def test_default_log_path_uses_node_name(self):
        ctx = MessageContext("coord-1")
        self.assertTrue(ctx.log_path.endswith("coord-1.clock.jsonl"))


if __name__ == "__main__":
    unittest.main()
