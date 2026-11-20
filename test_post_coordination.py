"""Tests for Post's coordination client and two-process lock behavior.

Fast contract tests run with unittest. The real two-Post-process test is enabled
with RUN_POST_COORDINATION_INTEGRATION=1 after the Chord ring, ZooKeeper,
coordinators, Post A, and Post B are running.
"""

import json
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import requests

from chord_client import ChordClient, ChordOutcomeUnknown
from coordination_client import CoordinationClient
from message_protocol import MessageContext


class FakeLeaderDiscovery:
    def __init__(self, leaders):
        self.leaders = list(leaders)
        self.calls = 0
        self.closed = False

    def start(self):
        pass

    def leader_info(self):
        self.calls += 1
        index = min(self.calls - 1, len(self.leaders) - 1)
        return dict(self.leaders[index], ready=True)

    def close(self):
        self.closed = True


class FakeResponse:
    def __init__(self, status, body, request_id):
        self.status_code = status
        self._body = body
        self.headers = {
            "X-Node-ID": "coordinator:test",
            "X-Request-ID": request_id,
            "X-Vector-Clock": '{"coordinator:test":1}',
        }

    def json(self):
        return self._body


class ContractSession:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


class CoordinationClientTests(unittest.TestCase):
    def setUp(self):
        self.ctx = MessageContext("post-test", boot_id="boot-1",
                                  log_path=os.devnull)

    def test_wait_retries_acquire_with_same_id_and_new_clock_sends(self):
        request_id = "operation-1"
        grant = {"resource": "post-write", "request_id": request_id,
                 "owner_id": self.ctx.node_id, "token": 8}
        session = ContractSession([
            FakeResponse(202, {"status": "waiting"}, request_id),
            FakeResponse(200, {"status": "granted", "grant": grant}, request_id),
        ])
        client = CoordinationClient(
            self.ctx, zk=FakeLeaderDiscovery([{"url": "http://coord:5101",
                                               "generation": 12}]),
            session=session, sleep=lambda _seconds: None)
        self.addCleanup(client.close)

        result = client.acquire("post-write", request_id)

        self.assertEqual(result, grant)
        self.assertEqual([call[0] for call in session.calls], [
            "http://coord:5101/locks/acquire", "http://coord:5101/locks/acquire"])
        sent = [call[1]["headers"] for call in session.calls]
        self.assertEqual([h["X-Request-ID"] for h in sent], [request_id, request_id])
        self.assertEqual([json.loads(h["X-Vector-Clock"])[self.ctx.node_id]
                          for h in sent], [1, 3])

    def test_acquire_rediscoveries_after_coordinator_failure(self):
        request_id = "operation-retry"
        grant = {"resource": "post-write", "request_id": request_id,
                 "owner_id": self.ctx.node_id, "token": 9}
        discovery = FakeLeaderDiscovery([
            {"url": "http://coord:5101", "generation": 12},
            {"url": "http://coord:5102", "generation": 13},
        ])
        session = ContractSession([
            requests.ConnectionError("old leader stopped"),
            FakeResponse(200, {"status": "granted", "grant": grant}, request_id),
        ])
        client = CoordinationClient(self.ctx, zk=discovery, session=session,
                                    sleep=lambda _seconds: None)
        self.addCleanup(client.close)

        self.assertEqual(client.acquire("post-write", request_id), grant)
        self.assertEqual([call[0] for call in session.calls], [
            "http://coord:5101/locks/acquire", "http://coord:5102/locks/acquire"])
        self.assertEqual([call[1]["headers"]["X-Request-ID"]
                          for call in session.calls], [request_id, request_id])
        self.assertEqual(discovery.calls, 2)

    def test_release_sends_complete_grant_and_preserves_operation_id(self):
        grant = {"resource": "post-write", "request_id": "write-44",
                 "owner_id": self.ctx.node_id, "token": 44}
        session = ContractSession([FakeResponse(
            200, {"status": "released"}, "write-44")])
        client = CoordinationClient(
            self.ctx, zk=FakeLeaderDiscovery([{"url": "http://coord:5101",
                                               "generation": 12}]),
            session=session, sleep=lambda _seconds: None)
        self.addCleanup(client.close)

        client.release(grant)

        self.assertEqual(session.calls[0][1]["json"], {"grant": grant})
        self.assertEqual(session.calls[0][1]["headers"]["X-Request-ID"], "write-44")

    def test_holder_can_release_same_grant_after_leader_replacement(self):
        request_id = "write-held-across-election"
        grant = {"resource": "post-write", "request_id": request_id,
                 "owner_id": self.ctx.node_id, "token": 51}
        discovery = FakeLeaderDiscovery([
            {"url": "http://coord:5101", "generation": 21},
            {"url": "http://coord:5102", "generation": 22},
        ])
        session = ContractSession([
            FakeResponse(200, {"status": "granted", "grant": grant}, request_id),
            FakeResponse(200, {"status": "released"}, request_id),
        ])
        client = CoordinationClient(self.ctx, zk=discovery, session=session,
                                    sleep=lambda _seconds: None)
        self.addCleanup(client.close)

        held = client.acquire("post-write", request_id)
        client.release(held)

        self.assertEqual(session.calls[0][0], "http://coord:5101/locks/acquire")
        self.assertEqual(session.calls[1][0], "http://coord:5102/locks/release")
        self.assertEqual(session.calls[1][1]["json"]["grant"], grant)


class ChordGrantTests(unittest.TestCase):
    def setUp(self):
        self.ctx = MessageContext("post-test", boot_id="boot-2", log_path=os.devnull)
        self.client = ChordClient(entry_peer="http://entry", message_context=self.ctx)

    def test_write_sends_grant_and_clock_metadata(self):
        lookup = Mock(status_code=200, ok=True, headers={
            "X-Node-ID": "peer:test", "X-Request-ID": "write-1",
            "X-Vector-Clock": '{"peer:test":1}',
        })
        lookup.json.return_value = {"success": True,
                                    "owner": {"url": "http://owner"}}
        put = Mock(status_code=200, ok=True, headers={
            "X-Node-ID": "peer:test", "X-Request-ID": "write-1",
            "X-Vector-Clock": '{"peer:test":2}',
        })
        put.json.return_value = {"success": True, "stored": True}
        self.client.session.post = Mock(side_effect=[lookup, put])
        grant = {"resource": "post-write", "request_id": "write-1",
                 "owner_id": self.ctx.node_id, "token": 3}

        self.client.put("post:7", self.client.routing_key(7), {"id": 7},
                        "write-1", coordination=grant)

        mutation = self.client.session.post.call_args_list[1].kwargs
        self.assertEqual(mutation["json"]["coordination"], grant)
        self.assertIn("X-Node-ID", mutation["headers"])
        self.assertIn("X-Vector-Clock", mutation["headers"])

    def test_mutation_timeout_is_reported_as_uncertain(self):
        self.client.lookup = Mock(return_value={"url": "http://owner"})
        self.client.session.post = Mock(side_effect=requests.Timeout("late reply"))
        with self.assertRaises(ChordOutcomeUnknown):
            self.client.put("post:7", self.client.routing_key(7), {"id": 7}, "write-2")


@unittest.skipUnless(os.getenv("RUN_POST_COORDINATION_INTEGRATION") == "1",
                     "set RUN_POST_COORDINATION_INTEGRATION=1 with both Post processes running")
class TwoPostProcessIntegrationTests(unittest.TestCase):
    """Exercise global ID allocation and like serialization across Post A/B."""

    @classmethod
    def setUpClass(cls):
        cls.urls = [os.getenv("POST_A_URL", "http://127.0.0.1:5002").rstrip("/"),
                    os.getenv("POST_B_URL", "http://127.0.0.1:5006").rstrip("/")]
        cls.ctx = MessageContext("post-test-client")

    def post(self, base, path, body):
        request_id = str(uuid.uuid4())
        headers = self.ctx.send_headers(request_id)
        # A create-post operation performs several serialized Chord round trips
        # under one grant; five seconds was too short on a local multi-process ring.
        response = requests.post(base + path, json=body, headers=headers, timeout=30)
        self.ctx.receive_headers(response.headers)
        return response

    def test_unique_ids_parallel_likes_and_duplicate_likes(self):
        users = [900000000 + uuid.uuid4().int % 1000000,
                 910000000 + uuid.uuid4().int % 1000000]
        created = [self.post(self.urls[i], "/posts", {
            "user_id": users[i], "content": "coordination integration",
            "caption": "post by separate Post process",
        }) for i in range(2)]
        self.assertEqual([r.status_code for r in created], [201, 201])
        ids = [r.json()["id"] for r in created]
        self.assertNotEqual(ids[0], ids[1])
        post_id = ids[0]

        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(lambda pair: self.post(
                self.urls[pair[0]], f"/posts/{post_id}/likes", {"user_id": pair[1]}),
                [(0, 920000001), (1, 920000002)]))
        self.assertEqual([r.status_code for r in responses], [201, 201])

        with ThreadPoolExecutor(max_workers=2) as pool:
            duplicate = list(pool.map(lambda base: self.post(
                base, f"/posts/{post_id}/likes", {"user_id": 920000003}), self.urls))
        self.assertCountEqual([r.status_code for r in duplicate], [201, 409])

        retrieved = self.post(self.urls[0], "/posts/retrieve", {"author": users[0]})
        self.assertEqual(retrieved.status_code, 200)
        stored = next(item for item in retrieved.json()["posts"] if item["id"] == post_id)
        self.assertEqual(set(stored["likes"]), {920000001, 920000002, 920000003})


if __name__ == "__main__":
    unittest.main()
