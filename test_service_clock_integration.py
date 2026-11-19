"""Member 6 tests: vector clocks in Broker, Profile, Notification and group_sender.

Everything runs over REAL localhost HTTP and TCP with Member 1's real
MessageContext. Each simulated process gets its own MessageContext (one clock
per process) and its own JSONL log in a temporary folder. Feed (Member 7) is
used unmodified as a real subscriber and as the real TCP group receiver, so
these tests also check the agreed wire format between Member 6 and Member 7.

Run:  python -m unittest test_service_clock_integration -v

What is checked (by vectors, never by wall clock or file order):
  * Profile request/reply clock propagation (success, 404, untracked, malformed)
  * Post event -> Broker -> Notification -> Profile causal chain
  * Multiple subscriber deliveries, retries (5xx and timeout), dedup on retry
  * TCP group messages carry "_meta"; acks are merged; untracked/malformed acks
  * Existing subscribe/unsubscribe/publish/notification behaviour still works
"""

import json
import logging
import os
import socketserver
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import requests
from flask import Flask, jsonify, request
from werkzeug.serving import make_server

import broker_server
import feed_server
import group_sender
import notification_server
import profile_server
from message_protocol import MessageContext


# ------------------------------------------------------------------ helpers
def happens_before(a, b):
    """Independent oracle: every a <= b and at least one strictly smaller."""
    keys = a.keys() | b.keys()
    return (all(a.get(k, 0) <= b.get(k, 0) for k in keys)
            and any(a.get(k, 0) < b.get(k, 0) for k in keys))


def read_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def post_event(event_id="evt-1", post_id=1, author=7):
    return {"event_id": event_id, "type": "post.created",
            "post": {"id": post_id, "author": author,
                     "timestamp": "2026-11-18T20:00:00+00:00",
                     "content": "photo.jpg", "caption": "Hi",
                     "likes": [], "comments": []}}


class ClockTestCase(unittest.TestCase):
    """Temp folder, per-'process' contexts, and real servers on free ports."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        # Cleanups run last-in-first-out, so this runs BEFORE the folder is
        # deleted. Windows cannot delete a log file that is still open.
        self.addCleanup(self.close_temp_log_files)
        self.contexts = {}
        # Fresh Profile/Notification state for every test (module-level apps).
        profile_server._profiles = {
            2: {"user_id": 2, "username": "user2", "display_name": "User 2", "bio": ""},
            7: {"user_id": 7, "username": "user7", "display_name": "User 7", "bio": ""},
            9: {"user_id": 9, "username": "user9", "display_name": "User 9", "bio": ""}}
        profile_server._following = {2: {7, 9}, 7: set(), 9: set()}
        patcher = patch.object(notification_server, "store",
                               notification_server.NotificationStore())
        patcher.start()
        self.addCleanup(patcher.stop)

    def close_temp_log_files(self):
        """Close and detach every logging FileHandler writing into self.dir."""
        folder = str(self.dir.resolve()).lower()
        loggers = [logging.getLogger()] + [
            item for item in logging.Logger.manager.loggerDict.values()
            if isinstance(item, logging.Logger)]
        for logger in loggers:
            for handler in list(logger.handlers):
                path = getattr(handler, "baseFilename", "")
                if path and str(Path(path).resolve()).lower().startswith(folder):
                    logger.removeHandler(handler)
                    handler.close()

    def ctx(self, name):
        """One MessageContext per simulated process, logging to the temp dir."""
        if name not in self.contexts:
            self.contexts[name] = MessageContext(
                name, log_path=str(self.dir / f"{name}.clock.jsonl"))
        return self.contexts[name]

    def log(self, name):
        return read_jsonl(self.dir / f"{name}.clock.jsonl")

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

    # Swap in the test's context for a module-level process clock.
    def use_profile(self, name="profile"):
        p = patch.object(profile_server, "CONTEXT", self.ctx(name))
        p.start()
        self.addCleanup(p.stop)
        return self.serve(profile_server.app)

    def use_notification(self, profile_url, name="notification"):
        for p in (patch.object(notification_server, "CONTEXT", self.ctx(name)),
                  patch.object(notification_server, "PROFILE_URL", profile_url)):
            p.start()
            self.addCleanup(p.stop)
        return self.serve(notification_server.app)

    def use_broker(self, name="broker", **options):
        options.setdefault("timeout", 1.0)
        options.setdefault("attempts", 3)
        options.setdefault("backoff", 0.01)
        app = broker_server.create_app(log_dir=str(self.dir), message_context=self.ctx(name),
                                       **options)
        return app, self.serve(app)

    def call(self, caller, method, url, body=None, request_id="rid-1", headers=None):
        """A tracked client call: one send, and one receive if a reply arrives."""
        sent = caller.send_headers(request_id, extra=headers or {})
        response = requests.request(method, url, json=body, headers=sent, timeout=10)
        caller.receive_headers(response.headers)
        return json.loads(sent["X-Vector-Clock"]), response

    def recorder(self, name, behaviour):
        """A test-double subscriber with its own clock that records every attempt.

        behaviour(attempt_number) -> (status, body) or "sleep:<seconds>".
        """
        context, attempts = self.ctx(name), []
        app = Flask(name)

        @app.post("/events")
        def events():
            rid = context.receive_headers(request.headers)
            attempts.append({"headers": dict(request.headers), "body": request.get_data(),
                             "merged": context.clock.snapshot()})
            action = behaviour(len(attempts))
            if isinstance(action, str) and action.startswith("sleep:"):
                time.sleep(float(action.split(":")[1]))
                action = (200, {"success": True})
            status, body = action
            reply = jsonify(body)
            reply.status_code = status
            reply.headers.update(context.send_headers(rid))
            return reply

        return self.serve(app) + "/events", attempts


# ------------------------------------------------------------------ Profile
class ProfileClockTests(ClockTestCase):

    def test_request_reply_propagates_clock_and_keeps_body(self):
        url = self.use_profile()
        caller = self.ctx("feed-test")
        sent, response = self.call(caller, "GET", url + "/profiles/2/following",
                                   request_id="follow-1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"user_id": 2, "following": [7, 9]})  # body unchanged
        self.assertEqual(response.headers["X-Node-ID"], self.ctx("profile").node_id)
        self.assertEqual(response.headers["X-Request-ID"], "follow-1")
        reply = json.loads(response.headers["X-Vector-Clock"])
        self.assertTrue(happens_before(sent, reply))                  # request -> reply
        self.assertTrue(happens_before(reply, caller.clock.snapshot()))  # reply -> receipt
        events = [e["event"] for e in self.log("profile")]
        self.assertEqual(events, ["HTTP_RECV", "HTTP_SEND"])
        self.assertTrue(all(e["request_id"] == "follow-1" for e in self.log("profile")))

    def test_error_reply_is_also_tracked(self):
        url = self.use_profile()
        caller = self.ctx("client-test")
        sent, response = self.call(caller, "GET", url + "/profiles/999", request_id="missing")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"], "profile not found")
        self.assertTrue(happens_before(sent, json.loads(response.headers["X-Vector-Clock"])))

    def test_untracked_input_is_served_and_logged_without_merge(self):
        url = self.use_profile()
        response = requests.get(url + "/profiles/7", headers={"X-Request-ID": "curl-1"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Request-ID"], "curl-1")
        vector = json.loads(response.headers["X-Vector-Clock"])
        self.assertEqual(vector, {self.ctx("profile").node_id: 1})    # only its own send
        self.assertEqual(self.log("profile")[0]["event"], "UNTRACKED_INPUT")

    def test_malformed_metadata_gets_400_and_no_merge(self):
        url = self.use_profile()
        response = requests.get(url + "/profiles/7", headers={
            "X-Node-ID": "evil:1", "X-Request-ID": "bad-1", "X-Vector-Clock": "{not json"})
        self.assertEqual(response.status_code, 400)
        vector = json.loads(response.headers["X-Vector-Clock"])
        self.assertNotIn("evil:1", vector)
        self.assertNotEqual(response.headers["X-Request-ID"], "")     # still tracked

    def test_mutation_endpoint_still_works_with_clock(self):
        url = self.use_profile()
        caller = self.ctx("client-test")
        _, response = self.call(caller, "POST", url + "/profiles/9/following",
                                {"following_user_id": 7}, request_id="follow-add")
        self.assertEqual(response.json(), {"success": True, "user_id": 9, "following": [7]})
        _, response = self.call(caller, "GET", url + "/profiles/7/followers", request_id="f")
        self.assertEqual(response.json()["followers"], [2, 9])


# ----------------------------------------------- Post -> Broker -> Notification
class BrokerNotificationTests(ClockTestCase):

    def start_chain(self):
        profile_url = self.use_profile()
        notification_url = self.use_notification(profile_url)
        app, broker_url = self.use_broker()
        notification_server.subscribe(broker_url, notification_url + "/events")
        return app, broker_url, notification_url

    def test_post_event_causal_chain_through_broker_to_notification(self):
        _, broker_url, notification_url = self.start_chain()
        post = self.ctx("post-a")
        sent, response = self.call(post, "POST", broker_url + "/publish", post_event(),
                                   request_id="create-post-1")
        report = response.json()
        self.assertTrue(report["all_delivered"])
        self.assertEqual(report["delivered"], 1)
        self.assertEqual(response.headers["X-Request-ID"], "create-post-1")

        # Notification for follower 2 (and not for the author) was created.
        notes = requests.get(notification_url + "/notifications/2").json()["notifications"]
        self.assertEqual([(n["type"], n["from_user_id"], n["post_id"]) for n in notes],
                         [("new_post", 7, 1)])

        rid = "create-post-1"
        broker = [e for e in self.log("broker") if e["request_id"] == rid]
        notif = [e for e in self.log("notification") if e["request_id"] == rid]
        prof = [e for e in self.log("profile") if e["request_id"] == rid]
        publish_recv = next(e for e in broker if e["event"] == "HTTP_RECV")
        delivery_send = next(e for e in broker if e["event"] == "HTTP_SEND" and "url" in e)
        events_recv = next(e for e in notif if e["event"] == "HTTP_RECV")
        lookup_send = next(e for e in notif if e["event"] == "HTTP_SEND" and "url" in e)
        profile_recv = next(e for e in prof if e["event"] == "HTTP_RECV")
        applied = next(e for e in notif if e["event"] == "EVENT_APPLIED")

        # Post send -> Broker receive -> Broker delivery send -> Notification
        # receive -> Profile lookup -> Profile receive; and event applied after.
        chain = [sent, publish_recv["vector_clock"], delivery_send["sent_vector_clock"],
                 events_recv["vector_clock"], lookup_send["sent_vector_clock"],
                 profile_recv["vector_clock"]]
        for earlier, later in zip(chain, chain[1:]):
            self.assertTrue(happens_before(earlier, later), (earlier, later))
        self.assertTrue(happens_before(events_recv["vector_clock"], applied["vector_clock"]))
        self.assertEqual(applied["event_id"], "evt-1")
        # Publisher's component is visible at Profile: causality crossed 3 hops.
        self.assertGreaterEqual(profile_recv["vector_clock"][post.node_id], sent[post.node_id])

    def test_one_clock_per_process(self):
        app, broker_url, _ = self.start_chain()
        self.assertIs(app.config["BROKER"].context, app.config["MESSAGE_CONTEXT"])
        self.call(self.ctx("post-a"), "POST", broker_url + "/publish", post_event())
        for name in ("broker", "notification", "profile"):
            ids = {e["node_id"] for e in self.log(name)}
            self.assertEqual(ids, {self.ctx(name).node_id}, name)
            own = [e["vector_clock"][self.ctx(name).node_id] for e in self.log(name)
                   if e["event"] != "UNTRACKED_INPUT"]
            self.assertEqual(own, sorted(own), f"{name} own component went backwards")

    def test_embedded_meta_is_stripped_and_not_merged(self):
        url, attempts = self.recorder("sub-a", lambda n: (200, {"success": True}))
        _, broker_url = self.use_broker()
        requests.post(broker_url + "/subscribe", json={"topic": "post.created", "url": url})
        event = dict(post_event(), _meta={"node_id": "fake:1", "request_id": "x",
                                          "vector_clock": {"fake:1": 99}})
        self.call(self.ctx("post-a"), "POST", broker_url + "/publish", event, request_id="m1")
        forwarded = json.loads(attempts[0]["body"])
        self.assertEqual(forwarded, post_event())                    # payload preserved
        self.assertNotIn("fake:1", self.ctx("broker").clock.snapshot())


# ---------------------------------------------- multiple subscribers + retries
class DeliveryRetryTests(ClockTestCase):

    def test_multiple_subscribers_retries_and_timeouts(self):
        flaky_url, flaky = self.recorder(
            "flaky", lambda n: (503, {"success": False}) if n == 1 else (200, {"success": True}))
        slow_url, slow = self.recorder(
            "slow", lambda n: "sleep:0.8" if n == 1 else (200, {"success": True}))
        # Member 7's real Feed app as a third subscriber (wire-format check).
        feed_ctx = self.ctx("feed")
        feed_app = feed_server.create_app(node_name="feed-m6-test", log_dir=str(self.dir),
                                          message_context=feed_ctx)
        feed_url = self.serve(feed_app) + "/events"
        _, broker_url = self.use_broker(timeout=0.4)
        for url in (flaky_url, slow_url, feed_url):
            requests.post(broker_url + "/subscribe", json={"topic": "post.created", "url": url})

        post = self.ctx("post-a")
        _, response = self.call(post, "POST", broker_url + "/publish", post_event("evt-r"),
                                request_id="retry-op")
        report = response.json()
        self.assertTrue(report["all_delivered"], report)
        by_url = {r["url"]: r for r in report["results"]}
        self.assertEqual(by_url[flaky_url]["attempts"], 2)
        self.assertEqual(by_url[slow_url]["attempts"], 2)
        self.assertEqual(by_url[feed_url]["attempts"], 1)

        # Retries: same body bytes, event_id and request ID; NEW send vector.
        for attempts in (flaky, slow):
            first, second = attempts
            self.assertEqual(first["body"], second["body"])
            self.assertEqual(json.loads(first["body"])["event_id"], "evt-r")
            self.assertEqual(first["headers"]["X-Request-Id"], "retry-op")
            self.assertEqual(second["headers"]["X-Request-Id"], "retry-op")
            v1 = json.loads(first["headers"]["X-Vector-Clock"])
            v2 = json.loads(second["headers"]["X-Vector-Clock"])
            self.assertTrue(happens_before(v1, v2))
        # The 503 reply was merged before the retry was sent.
        flaky_reply_owner = self.ctx("flaky").node_id
        self.assertIn(flaky_reply_owner, json.loads(flaky[1]["headers"]["X-Vector-Clock"]))

        sends = [e for e in self.log("broker") if e["event"] == "HTTP_SEND" and "url" in e]
        recvs = [e for e in self.log("broker") if e["event"] == "HTTP_RECV" and "url" in e]
        self.assertEqual(len(sends), 5)                              # 2 + 2 + 1 attempts
        vectors = [json.dumps(e["sent_vector_clock"], sort_keys=True) for e in sends]
        self.assertEqual(len(set(vectors)), 5)                       # every send distinct
        slow_recv = [e["attempt"] for e in recvs if e["url"] == slow_url]
        self.assertEqual(slow_recv, [2])                             # timeout -> no receive

        # Feed (Member 7) received the unchanged event and merged our clock.
        self.assertIn(1, feed_app.config["FEED_SERVICE"].received_posts)
        self.assertIn(self.ctx("broker").node_id, feed_ctx.clock.snapshot())

    def test_notification_dedups_redelivery_by_event_id(self):
        profile_url = self.use_profile()
        notification_url = self.use_notification(profile_url)
        _, broker_url = self.use_broker()
        notification_server.subscribe(broker_url, notification_url + "/events")
        post = self.ctx("post-a")
        self.call(post, "POST", broker_url + "/publish", post_event("same"), request_id="p1")
        _, again = self.call(post, "POST", broker_url + "/publish", post_event("same"),
                             request_id="p2")
        self.assertIs(again.json()["results"][0]["duplicate"], True)
        notes = requests.get(notification_url + "/notifications/2").json()["notifications"]
        self.assertEqual(len(notes), 1)

    def test_notification_502_when_profile_down_makes_broker_retry(self):
        # "Profile is down": a server that answers 503 immediately. (A closed
        # port is unreliable: Windows waits ~2 s before refusing a connection.)
        broken = Flask("broken-profile")

        @broken.route("/profiles/<int:user_id>/followers")
        def unavailable(user_id):
            return jsonify({"success": False, "error": "down"}), 503

        notification_url = self.use_notification(self.serve(broken))
        _, broker_url = self.use_broker(timeout=5.0)
        notification_server.subscribe(broker_url, notification_url + "/events")
        _, response = self.call(self.ctx("post-a"), "POST", broker_url + "/publish",
                                post_event(), request_id="down")
        result = response.json()["results"][0]
        self.assertEqual((result["status"], result["attempts"]), (502, 3))
        replies = [e for e in self.log("notification")
                   if e["event"] == "HTTP_SEND" and e.get("status") == 502]
        self.assertEqual(len(replies), 3)                            # errors are tracked


# --------------------------------------------------------------- TCP group
class GroupMessageTests(ClockTestCase):

    def run_sender(self, members, message_id="group-demo-1"):
        args = ["--message-id", message_id, "--timeout", "2", "--log-dir", str(self.dir)]
        for name, port in members:
            args += ["--member", f"{name}=127.0.0.1:{port}"]
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NODE_NAME", None)
            return group_sender.main(args)

    def feed_receiver(self, name):
        server = feed_server.start_group_receiver("127.0.0.1", 0, name, str(self.dir),
                                                  message_context=self.ctx(name))
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_address[1]

    def raw_receiver(self, reply_for):
        """Records the exact TCP line; reply_for(message) builds the ack dict."""
        lines = []

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                line = self.rfile.readline(65537)
                lines.append(line)
                reply = reply_for(json.loads(line))
                self.wfile.write(json.dumps(reply).encode() + b"\n")

        server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_address[1], lines

    def test_group_send_to_real_feed_receivers(self):
        ports = [("feed", self.feed_receiver("feed")), ("feed-2", self.feed_receiver("feed-2"))]
        self.assertEqual(self.run_sender(ports), 0)
        log = read_jsonl(self.dir / "group-sender.clock.jsonl")
        sends = [e for e in log if e["event"] == "GROUP_SEND"]
        recvs = [e for e in log if e["event"] == "GROUP_RECV"]
        self.assertEqual(len(sends), 2)
        self.assertEqual(len(recvs), 2)
        self.assertNotEqual(sends[0]["sent_vector_clock"], sends[1]["sent_vector_clock"])
        self.assertTrue(all(e["request_id"] == "group-demo-1" for e in log))
        sender_id = sends[0]["node_id"]
        for name in ("feed", "feed-2"):
            self.assertIn(sender_id, self.ctx(name).clock.snapshot())   # receiver merged
            feed_log = self.log(name)
            self.assertEqual([e["event"] for e in feed_log], ["GROUP_RECV", "GROUP_SEND"])
        # Ack merged: sender's final clock knows both Feed nodes.
        final = recvs[-1]["vector_clock"]
        for name in ("feed", "feed-2"):
            self.assertIn(self.ctx(name).node_id, final)

    def test_wire_keeps_message_structure_and_adds_meta(self):
        ack_ctx = self.ctx("fake-feed")

        def ack(message):
            app, rid = ack_ctx.receive_payload(message)
            return ack_ctx.send_payload({"success": True, "node": "a",
                                         "message_id": app["message_id"], "status": "ready"}, rid)
        port_a, lines_a = self.raw_receiver(ack)
        port_b, lines_b = self.raw_receiver(
            lambda m: ack_ctx.send_payload({"success": True, "node": "b",
                                            "message_id": m["message_id"], "status": "ready"},
                                           m["_meta"]["request_id"]))
        self.assertEqual(self.run_sender([("a", port_a), ("b", port_b)]), 0)
        for line in lines_a + lines_b:
            message = json.loads(line)
            meta = message.pop("_meta")
            self.assertEqual(message, {"type": "demo_status_request", "group": "service_nodes",
                                       "message_id": "group-demo-1"})
            self.assertEqual(set(meta), {"node_id", "request_id", "vector_clock"})
            self.assertEqual(meta["request_id"], "group-demo-1")

    def test_legacy_ack_without_meta_is_accepted_but_untracked(self):
        legacy = lambda m: {"success": True, "node": "old", "message_id": m["message_id"],
                            "status": "ready"}
        port_a, _ = self.raw_receiver(legacy)
        port_b, _ = self.raw_receiver(lambda m: dict(legacy(m), node="old2"))
        self.assertEqual(self.run_sender([("old", port_a), ("old2", port_b)]), 0)
        events = [e["event"] for e in read_jsonl(self.dir / "group-sender.clock.jsonl")]
        self.assertEqual(events.count("UNTRACKED_INPUT"), 2)
        self.assertNotIn("GROUP_RECV_FAILED", events)

    def test_malformed_ack_meta_is_a_failure(self):
        bad = lambda m: {"success": True, "node": "bad", "message_id": m["message_id"],
                         "status": "ready", "_meta": {"node_id": "x"}}
        good_port = self.feed_receiver("feed")
        bad_port, _ = self.raw_receiver(bad)
        self.assertEqual(self.run_sender([("feed", good_port), ("bad", bad_port)]), 1)

    def test_invalid_request_id_message_id_gets_uuid_request_id(self):
        ports = [("feed", self.feed_receiver("feed")), ("feed-2", self.feed_receiver("feed-2"))]
        self.assertEqual(self.run_sender(ports, message_id="has spaces"), 0)
        rids = {e["request_id"] for e in read_jsonl(self.dir / "group-sender.clock.jsonl")}
        self.assertEqual(len(rids), 1)
        self.assertNotIn("has spaces", rids)


# ---------------------------------------------------- existing behaviour kept
class ExistingBehaviourTests(ClockTestCase):

    def test_subscription_table_and_untracked_publish_still_work(self):
        profile_url = self.use_profile()
        notification_url = self.use_notification(profile_url)
        _, broker_url = self.use_broker()
        notification_server.subscribe(broker_url, notification_url + "/events")
        notification_server.subscribe(broker_url, notification_url + "/events")  # idempotent
        subs = requests.get(broker_url + "/subscriptions").json()["subscriptions"]
        self.assertEqual(len(subs), 3)                               # 3 topics, no dupes

        # Milestone 3 style publisher: no clock headers at all.
        report = requests.post(broker_url + "/publish", json=post_event("legacy"),
                               headers={"X-Request-ID": "legacy-1"}).json()
        self.assertTrue(report["all_delivered"])
        broker_events = [e["event"] for e in self.log("broker")]
        self.assertIn("UNTRACKED_INPUT", broker_events)
        # The delivery the broker made is still tracked (broker is a process).
        recv = [e for e in self.log("notification")
                if e["request_id"] == "legacy-1" and e["event"] == "HTTP_RECV"]
        self.assertIn(self.ctx("broker").node_id, recv[0]["vector_clock"])

        removed = requests.post(broker_url + "/unsubscribe", json={
            "topic": "post.liked", "url": notification_url + "/events"}).json()
        self.assertIs(removed["removed"], True)

    def test_like_event_and_direct_notification_dedup(self):
        profile_url = self.use_profile()
        notification_url = self.use_notification(profile_url)
        _, broker_url = self.use_broker()
        notification_server.subscribe(broker_url, notification_url + "/events")
        post = self.ctx("post-b")
        self.call(post, "POST", broker_url + "/publish",
                  {"event_id": "like-1", "type": "post.liked", "post_id": 1,
                   "post_author": 7, "user_id": 2}, request_id="like-op")
        body = {"to_user_id": 7, "type": "follow", "from_user_id": 9}
        _, first = self.call(post, "POST", notification_url + "/notifications", body,
                             request_id="direct-1")
        _, second = self.call(post, "POST", notification_url + "/notifications", body,
                              request_id="direct-1")
        self.assertEqual((first.status_code, second.status_code), (201, 200))
        self.assertIs(second.json()["duplicate"], True)
        notes = requests.get(notification_url + "/notifications/7").json()["notifications"]
        self.assertEqual(sorted(n["type"] for n in notes), ["follow", "like"])

    def test_bad_event_and_bad_clock_are_400_with_tracked_reply(self):
        _, broker_url = self.use_broker()
        _, bad = self.call(self.ctx("post-a"), "POST", broker_url + "/publish",
                           {"type": "post.created"}, request_id="bad-event")
        self.assertEqual(bad.status_code, 400)
        self.assertIn("X-Vector-Clock", bad.headers)
        bad_clock = requests.post(broker_url + "/publish", json=post_event(), headers={
            "X-Node-ID": "p:1", "X-Request-ID": "r", "X-Vector-Clock": '{"p:1": -1}'})
        self.assertEqual(bad_clock.status_code, 400)
        self.assertNotIn("p:1", self.ctx("broker").clock.snapshot())

    def test_missing_module_cannot_silently_disable_milestone_4(self):
        with patch.dict("sys.modules", {"message_protocol": None}), \
                patch.dict(os.environ, {"COORDINATION_ENABLED": "1"}):
            for factory in (broker_server.process_context, profile_server.process_context,
                            notification_server.process_context):
                with self.assertRaisesRegex(RuntimeError, "Member 1"):
                    factory()
            with self.assertRaisesRegex(RuntimeError, "Member 1"):
                group_sender.make_context(str(self.dir))
        with patch.dict("sys.modules", {"message_protocol": None}), \
                patch.dict(os.environ, {"COORDINATION_ENABLED": "0"}):
            self.assertIsNone(broker_server.process_context())        # Milestone 3 mode


if __name__ == "__main__":
    unittest.main()
