"""Member 7 tests. Contract doubles test wiring, NOT teammate implementations.

Real-cluster test: set RUN_COORDINATION_INTEGRATION=1, then run this module.
It fails (rather than skips) when explicitly enabled but dependencies are absent.
"""

import io
import json
import logging
import os
import socket
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from flask import Flask, jsonify, request
from werkzeug.serving import make_server

import client
import demo_coordination as demo
import feed_server as feed


class ContractContext:
    """Test-only MessageContext stand-in to observe transport boundaries."""

    def __init__(self, name="feed:test"):
        self.node_id = name
        self.counts = {name: 0}
        self.calls = []
        self.lock = threading.Lock()

    def send_headers(self, request_id, extra=None):
        with self.lock:
            self.counts[self.node_id] += 1
            self.calls.append(("send", request_id))
            return dict(extra or {}, **{"X-Node-ID": self.node_id,
                        "X-Request-ID": request_id, "X-Vector-Clock": json.dumps(self.counts)})

    def receive_headers(self, headers):
        with self.lock:
            vector = json.loads(headers.get("X-Vector-Clock", "{}"))
            if not isinstance(vector, dict) or any(type(v) is not int or v < 0 for v in vector.values()):
                raise ValueError("invalid vector")
            for key, value in vector.items():
                self.counts[key] = max(self.counts.get(key, 0), value)
            self.counts[self.node_id] += 1
            rid = headers.get("X-Request-ID") or "untracked-test"
            self.calls.append(("receive", rid))
            return rid

    def send_payload(self, payload, request_id):
        headers = self.send_headers(request_id)
        return dict(payload, _meta={"node_id": self.node_id, "request_id": request_id,
                                   "vector_clock": json.loads(headers["X-Vector-Clock"])})

    def receive_payload(self, payload):
        meta = payload.get("_meta", {})
        rid = self.receive_headers({"X-Request-ID": meta.get("request_id", "untracked-test"),
                                   "X-Vector-Clock": json.dumps(meta.get("vector_clock", {}))})
        return {key: value for key, value in payload.items() if key != "_meta"}, rid

    def log(self, event, request_id, **fields):
        self.calls.append((event, request_id))


def response(body, status=200, node="dependency:test", tick=4, rid="operation"):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body).encode()
    result.headers.update({"X-Node-ID": node, "X-Request-ID": rid,
                           "X-Vector-Clock": json.dumps({node: tick})})
    return result


def post(post_id=1, timestamp="2026-01-01T00:00:00Z"):
    return {"id": post_id, "author": 7, "timestamp": timestamp,
            "content": "photo.jpg", "caption": "caption", "likes": [], "comments": []}


class FeedClockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.context = ContractContext()
        name = "test-feed-" + Path(self.tmp.name).name
        self.app = feed.create_app(log_dir=self.tmp.name, node_name=name, message_context=self.context)
        self.http = self.app.test_client()
        self.headers = {"X-Node-ID": "client:test", "X-Request-ID": "operation",
                        "X-Vector-Clock": '{"client:test": 8}'}
        self.addCleanup(self.close_logger, name)

    @staticmethod
    def close_logger(name):
        logger = logging.getLogger(name)
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    def test_feed_propagates_clock_and_keeps_physical_time_sorting(self):
        older, newer = post(1), post(2, "2026-01-01T01:00:00+00:00")
        with patch.object(feed.requests, "request", side_effect=[
            response({"user_id": 2, "following": [7, 7]}, node="profile:test"),
            response({"success": True, "posts": [older, newer, older]}, node="post:test"),
        ]) as network:
            result = self.http.get("/feed/2", headers=self.headers)
        self.assertEqual(result.status_code, 200)
        self.assertEqual([p["id"] for p in result.json["posts"]], [2, 1])
        self.assertEqual(network.call_count, 2)
        first = json.loads(network.call_args_list[0].kwargs["headers"]["X-Vector-Clock"])
        second = json.loads(network.call_args_list[1].kwargs["headers"]["X-Vector-Clock"])
        final = json.loads(result.headers["X-Vector-Clock"])
        self.assertEqual(first["client:test"], 8)
        self.assertEqual(second["profile:test"], 4)
        self.assertEqual(final["post:test"], 4)
        self.assertTrue(demo.happens_before(first, second))
        self.assertTrue(demo.happens_before(second, final))
        self.assertEqual(result.headers["X-Request-ID"], "operation")

    def test_dependency_error_clock_is_merged_before_returning_error(self):
        with patch.object(feed.requests, "request", return_value=response(
                {"error": "offline"}, 503, tick=29)):
            result = self.http.get("/feed/2", headers=self.headers)
        self.assertEqual(result.status_code, 503)
        self.assertEqual(json.loads(result.headers["X-Vector-Clock"])["dependency:test"], 29)

    def test_timeout_response_is_timestamped_and_does_not_return_partial_feed(self):
        with patch.object(feed.requests, "request", side_effect=requests.Timeout()):
            result = self.http.get("/feed/2", headers=self.headers)
        self.assertEqual(result.status_code, 504)
        self.assertNotIn("posts", result.json)
        self.assertIn("X-Vector-Clock", result.headers)

    def test_invalid_clock_returns_timestamped_400_without_dependency_call(self):
        with patch.object(feed.requests, "request") as network:
            result = self.http.get("/feed/2", headers=dict(self.headers, **{"X-Vector-Clock": "bad"}))
        self.assertEqual(result.status_code, 400)
        self.assertIn("X-Vector-Clock", result.headers)
        network.assert_not_called()

    def test_event_retry_deduplicates_business_data_not_delivery_metadata(self):
        event = {"type": "post.created", "event_id": "event-1", "post": post(),
                 "_meta": {"vector_clock": {"broker": 1}}}
        first = self.http.post("/events", json=event, headers=self.headers)
        event["_meta"]["vector_clock"]["broker"] = 99
        second = self.http.post("/events", json=event, headers=self.headers)
        self.assertFalse(first.json["duplicate"])
        self.assertTrue(second.json["duplicate"])
        self.assertEqual([c[0] for c in self.context.calls].count("receive"), 2)
        self.assertNotIn("broker", self.context.counts)  # HTTP headers merged once.
        event["post"]["caption"] = "different data under the same event ID"
        self.assertEqual(self.http.post("/events", json=event, headers=self.headers).status_code, 400)

    def test_group_receiver_shares_http_clock_and_stamps_errors(self):
        name = "tcp-" + Path(self.tmp.name).name
        server = feed.start_group_receiver("127.0.0.1", 0, name, self.tmp.name, self.context)
        self.addCleanup(self.close_logger, name)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.http.get("/health", headers=self.headers)
        for valid in (True, False):
            payload = {"group": "service_nodes", "type": "demo_status_request" if valid else "bad",
                       "message_id": "m1", "_meta": {"node_id": "group:test", "request_id": "group-op",
                                                       "vector_clock": {"group:test": 20}}}
            with socket.create_connection(server.server_address, timeout=3) as conn:
                conn.sendall(json.dumps(payload).encode() + b"\n")
                with conn.makefile("rb") as stream:
                    reply = json.loads(stream.readline())
            self.assertEqual(reply["success"], valid)
            self.assertEqual(reply["_meta"]["node_id"], self.context.node_id)
            self.assertEqual(reply["_meta"]["vector_clock"]["client:test"], 8)
            self.assertEqual(reply["_meta"]["vector_clock"]["group:test"], 20)

    def test_subscription_uses_same_context(self):
        with patch.object(feed.requests, "request", return_value=response({"success": True})) as network:
            feed.subscribe("http://broker", "http://feed/events", logging.getLogger("test"), self.context)
        self.assertEqual(network.call_args.kwargs["headers"]["X-Node-ID"], self.context.node_id)
        self.assertEqual(self.context.counts["dependency:test"], 4)

    def test_missing_module_cannot_silently_disable_milestone_4(self):
        with patch.dict("sys.modules", {"message_protocol": None}), \
                patch.object(feed, "_process_context", None), \
                patch.dict(os.environ, {"COORDINATION_ENABLED": "1"}):
            with self.assertRaisesRegex(RuntimeError, "Member 1"):
                feed.process_context()


class ClientClockTests(unittest.TestCase):
    def setUp(self):
        self.context = ContractContext("client:test")
        self.patch_context = patch.object(client, "_process_context", self.context)
        self.patch_context.start()
        self.addCleanup(self.patch_context.stop)

    @staticmethod
    def success():
        result = Mock()
        result.__enter__ = Mock(return_value=result)
        result.__exit__ = Mock(return_value=False)
        result.status = 200
        result.headers = {"X-Request-ID": "operation", "X-Vector-Clock": '{"server:test": 9}'}
        result.read.return_value = b'{"success":true}'
        return result

    def test_get_retry_increments_send_clock_and_keeps_request_id(self):
        with patch.object(client.urllib.request, "urlopen", side_effect=[
                urllib.error.URLError("temporary"), self.success()]) as network:
            result = client.send_request("feed", "GET", "/feed/2", request_id="operation")
        self.assertTrue(result["success"])
        sent = [dict((k.lower(), v) for k, v in call.args[0].header_items())
                for call in network.call_args_list]
        self.assertEqual([h["x-request-id"] for h in sent], ["operation", "operation"])
        self.assertEqual([json.loads(h["x-vector-clock"])["client:test"] for h in sent], [1, 2])
        self.assertEqual(self.context.counts["server:test"], 9)

    def test_write_timeout_is_not_retried(self):
        with patch.object(client.urllib.request, "urlopen", side_effect=urllib.error.URLError("timeout")) as network:
            result = client.send_request("post", "POST", "/posts", {"caption": "x"})
        self.assertEqual(network.call_count, 1)
        self.assertIn("may have completed", result["error"])

    def test_http_error_clock_merged_and_not_retried(self):
        error = urllib.error.HTTPError("http://post", 409, "Conflict",
                                      {"X-Vector-Clock": '{"server:test": 17}'}, io.BytesIO(b"duplicate"))
        with patch.object(client.urllib.request, "urlopen", side_effect=error) as network:
            result = client.send_request("post", "POST", "/posts/1/likes", {"user_id": 2})
        self.assertEqual(result["status"], 409)
        self.assertEqual(network.call_count, 1)
        self.assertEqual(self.context.counts["server:test"], 17)

    def test_post_url_option_targets_second_instance(self):
        with patch.dict(client.SERVICES), patch.object(client, "like_post", return_value={"success": True}) as like:
            with patch("sys.stdout", new=io.StringIO()):
                code = client.main(["--post-url", "http://127.0.0.1:5006/", "like", "1", "9"])
            self.assertEqual(code, 0)
            self.assertEqual(client.SERVICES["post"], "http://127.0.0.1:5006")
            like.assert_called_once_with(1, 9)


class RealSharedModuleTests(unittest.TestCase):
    """Exercise the merged Member 1 module over real localhost HTTP and TCP."""

    def setUp(self):
        from message_protocol import MessageContext
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.contexts = {}
        for name in ("feed", "client", "dependency", "group"):
            self.contexts[name] = MessageContext(
                "real-" + name, log_path=str(Path(self.tmp.name) / (name + ".jsonl")))
        self.logger_name = "real-feed-" + Path(self.tmp.name).name
        self.addCleanup(FeedClockTests.close_logger, self.logger_name)

    def serve(self, app):
        server = make_server("127.0.0.1", 0, app, threaded=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def stop():
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
        self.addCleanup(stop)
        return f"http://127.0.0.1:{server.server_port}"

    def test_client_feed_dependency_round_trip_uses_real_clocks(self):
        ctx = self.contexts["dependency"]
        backend = Flask("dependency")

        @backend.route("/profiles/2/following")
        @backend.route("/posts/retrieve", methods=["POST"])
        def reply():
            rid = ctx.receive_headers(request.headers)
            body = ({"user_id": 2, "following": [7]} if request.method == "GET"
                    else {"success": True, "posts": [post()]})
            result = jsonify(body)
            result.headers.update(ctx.send_headers(rid))
            return result

        dependency_url = self.serve(backend)
        app = feed.create_app(profile_url=dependency_url, post_url=dependency_url,
                              node_name=self.logger_name, log_dir=self.tmp.name,
                              message_context=self.contexts["feed"])
        feed_url = self.serve(app)
        caller = self.contexts["client"]
        with patch.object(client, "_process_context", caller), patch.dict(client.SERVICES, {"feed": feed_url}):
            result = client.send_request("feed", "GET", "/feed/2", request_id="real-http")
        self.assertEqual(result["posts"][0]["id"], 1)
        vector = caller.clock.snapshot()
        self.assertGreater(vector[ctx.node_id], 0)
        self.assertGreater(vector[self.contexts["feed"].node_id], 0)
        self.assertEqual(vector[caller.node_id], 2)  # exactly one send and one receive
        wire = demo.Wire(caller)
        code, _ = wire.call("GET", feed_url + "/feed/2")
        self.assertEqual(code, 200)
        self.assertEqual(len(wire.exchanges), 1)
        self.assertTrue(demo.read_events(self.tmp.name))

    def test_malformed_request_id_returns_400_not_header_generation_500(self):
        app = feed.create_app(node_name=self.logger_name, log_dir=self.tmp.name,
                              message_context=self.contexts["feed"])
        result = app.test_client().get("/health", headers={
            "X-Node-ID": "client:test", "X-Vector-Clock": '{"client:test": 1}',
            "X-Request-ID": "invalid id with spaces"})
        self.assertEqual(result.status_code, 400)
        self.assertNotIn(" ", result.headers["X-Request-ID"])
        self.contexts["client"].receive_headers(result.headers)

    def test_real_group_exchange_shares_feed_clock(self):
        ctx, sender = self.contexts["feed"], self.contexts["group"]
        app = feed.create_app(node_name=self.logger_name, log_dir=self.tmp.name, message_context=ctx)
        app.test_client().get("/health", headers=self.contexts["client"].send_headers("http-first"))
        server = feed.start_group_receiver("127.0.0.1", 0, self.logger_name, self.tmp.name, ctx)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        message = sender.send_payload({"group": "service_nodes", "type": "demo_status_request",
                                       "message_id": "real-group"}, "real-group")
        with socket.create_connection(server.server_address, timeout=3) as connection:
            connection.sendall(json.dumps(message).encode() + b"\n")
            with connection.makefile("rb") as stream:
                response_payload, rid = sender.receive_payload(json.loads(stream.readline()))
        self.assertTrue(response_payload["success"])
        self.assertEqual(rid, "real-group")
        self.assertIn(self.contexts["client"].node_id, sender.clock.snapshot())
        self.assertEqual(sender.clock.snapshot()[ctx.node_id], 4)


class EvidenceTests(unittest.TestCase):
    @staticmethod
    def event(rid, name, node, vector, token):
        return {"request_id": rid, "event": name, "node_id": node,
                "vector_clock": vector, "grant_token": token}

    def test_sections_are_checked_by_causality_not_file_order(self):
        e = self.event
        events = [e("b", "CS_EXIT", "b", {"a": 2, "b": 2}, 2),
                  e("a", "CS_ENTER", "a", {"a": 1}, 1),
                  e("b", "CS_ENTER", "b", {"a": 2, "b": 1}, 2),
                  e("a", "CS_EXIT", "a", {"a": 2}, 1)]
        self.assertEqual(len(demo.check_critical_sections(events, ["a", "b"])), 2)
        events[2]["vector_clock"] = {"b": 1}
        with self.assertRaisesRegex(demo.DemoFailure, "exclusive"):
            demo.check_critical_sections(events, ["a", "b"])

    def test_missing_evidence_does_not_pass(self):
        with self.assertRaises(demo.DemoFailure):
            demo.check_critical_sections([], ["missing"])
        with self.assertRaises(demo.DemoFailure):
            demo.check_concurrent_origins([], ["a", "b"])

    def test_independent_vs_sequential_client_origins(self):
        events = [{"request_id": "a", "event": "HTTP_SEND", "node_id": "client-a:boot",
                   "url": "http://post/posts/1/likes", "vector_clock": {"a": 1}},
                  {"request_id": "b", "event": "HTTP_SEND", "node_id": "client-b:boot",
                   "url": "http://post/posts/1/likes", "vector_clock": {"b": 1}}]
        demo.check_concurrent_origins(events, ["a", "b"])
        events[1]["vector_clock"] = {"a": 1, "b": 1}
        with self.assertRaisesRegex(demo.DemoFailure, "not concurrent"):
            demo.check_concurrent_origins(events, ["a", "b"])

    def test_preflight_reports_missing_components(self):
        with patch.object(demo.importlib.util, "find_spec", return_value=None):
            with self.assertRaisesRegex(demo.DemoFailure, "BLOCKED: missing message_protocol"):
                demo.preflight()


@unittest.skipUnless(os.getenv("RUN_COORDINATION_INTEGRATION") == "1",
                     "Real cluster opt-in: RUN_COORDINATION_INTEGRATION=1; needs teammates + ZooKeeper")
class RealCoordinationTests(unittest.TestCase):
    def test_real_cluster(self):
        summary = demo.run_demo(hosts=os.getenv("ZK_HOSTS", "127.0.0.1:2181"))
        self.assertEqual(summary["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
