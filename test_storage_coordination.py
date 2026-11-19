"""Peer HTTP enforcement tests using an in-memory authoritative grant stub."""

import tempfile
import unittest

import chord_peer
from message_protocol import MessageContext
from peer_storage import PeerStorage


class FakePeer:
    def __init__(self, db_dir):
        self.id = 20
        self.coordination_enabled = True
        self.me = {"id": 20, "url": "http://peer"}
        self.storage = PeerStorage(20, db_dir)
        self.message_context = MessageContext("test-peer", log_path=db_dir + "/clock.jsonl")
        self.grant = {"resource": "post-write", "request_id": "op-1",
                      "owner_id": "post-a:test", "token": 3}
        self.active = True

    def info(self, *args, **kwargs):
        pass

    def snapshot(self):
        return (self.me, [self.me], [], "ready", None)

    def writes_allowed(self):
        return True, None

    def grant_is_authoritative(self, grant):
        return self.active and grant == self.grant

    def call(self, peer, path, body=None, request_id=None, **kwargs):
        return {"success": True, "imported": len(body.get("records", []))}


class StorageCoordinationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.old_peer = chord_peer.PEER
        chord_peer.PEER = FakePeer(self.temp.name)
        self.addCleanup(setattr, chord_peer, "PEER", self.old_peer)
        self.client = chord_peer.app.test_client()
        self.valid = {"record_id": "post:1", "routing_key": chord_peer.routing_key_for("post:1"),
                      "value": {"id": 1, "content": "hello"}, "operation_id": "op-1",
                      "coordination": chord_peer.PEER.grant}

    def put(self, payload):
        return self.client.post("/records/put", json=payload)

    def test_missing_and_wrong_owner_grants_do_not_modify_storage(self):
        missing = dict(self.valid)
        missing.pop("coordination")
        self.assertEqual(self.put(missing).status_code, 503)
        wrong = dict(self.valid, coordination=dict(self.valid["coordination"],
                                                    owner_id="post-b:test"))
        self.assertEqual(self.put(wrong).status_code, 503)
        self.assertIsNone(chord_peer.PEER.storage.get("post:1"))

    def test_released_grant_is_rejected_and_valid_grant_writes_token(self):
        chord_peer.PEER.active = False
        self.assertEqual(self.put(self.valid).status_code, 503)
        self.assertIsNone(chord_peer.PEER.storage.get("post:1"))
        chord_peer.PEER.active = True
        self.assertEqual(self.put(self.valid).status_code, 200)
        self.assertEqual(chord_peer.PEER.storage.get("post:1")["coordination_token"], 3)

    def test_stale_token_rejection_preserves_existing_record(self):
        chord_peer.PEER.storage.put("post:1", self.valid["routing_key"],
                                    self.valid["value"], "initial", 4)
        payload = dict(self.valid, operation_id="op-2")
        self.assertEqual(self.put(payload).status_code, 409)
        saved = chord_peer.PEER.storage.get("post:1")
        self.assertEqual(saved["value"]["content"], "hello")
        self.assertEqual(saved["coordination_token"], 4)


if __name__ == "__main__":
    unittest.main()
