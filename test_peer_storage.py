"""LOCAL storage tests only: no network, no peer processes.

    python3 -m unittest test_peer_storage -v

Tests that use real peer processes are in test_p2p_integration.py.
"""

import tempfile
import unittest

from peer_storage import PeerStorage


def post(post_id, text="x"):
    return {"id": post_id, "author": 7, "timestamp": "2026-11-03T00:00:00+00:00",
            "content": text, "caption": "", "likes": [], "comments": []}


class PeerStorageTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.store = PeerStorage(20, self.dir.name)

    def test_restart_keeps_records_and_each_peer_has_its_own_file(self):
        self.store.put("post:1", 17, post(1), "op1")
        again = PeerStorage(20, self.dir.name)
        self.assertEqual(again.get("post:1")["value"], post(1))
        self.assertNotEqual(PeerStorage(28, self.dir.name).db_path, again.db_path)
        self.assertIsNone(PeerStorage(28, self.dir.name).get("post:1"))

    def test_colliding_routing_keys_do_not_overwrite(self):
        self.store.put("post:1", 17, post(1, "a"))
        self.store.put("post:2", 17, post(2, "b"))
        self.assertEqual(self.store.get("post:1")["value"]["content"], "a")
        self.assertEqual(self.store.get("post:2")["value"]["content"], "b")

    def test_non_post_records_are_stored(self):
        self.store.put("author:7", 5, {"post_ids": [1, 2]})
        self.store.put("meta:next_post_id", 9, {"value": 3})
        self.assertEqual(self.store.get("author:7")["value"], {"post_ids": [1, 2]})
        self.assertEqual(self.store.get("meta:next_post_id")["value"], {"value": 3})

    def test_same_operation_id_is_harmless_new_one_bumps_version(self):
        self.assertEqual(self.store.put("post:1", 17, post(1), "a")["version"], 1)
        self.assertEqual(self.store.put("post:1", 17, post(1), "a")["version"], 1)
        self.assertEqual(self.store.put("post:1", 17, post(1), "b")["version"], 2)

    def test_stale_and_repeated_imports_change_nothing(self):
        newer = dict(self.store.put("post:1", 17, post(1, "new"), "a"), version=2)
        older = dict(newer, version=1, value=post(1, "old"))
        self.assertEqual(self.store.import_records([newer], "backup"), (1, 0))
        self.assertEqual(self.store.import_records([newer], "backup"), (0, 1))
        self.assertEqual(self.store.import_records([older], "backup"), (0, 1))
        self.assertEqual(self.store.get("post:1")["value"]["content"], "new")

    def test_delete_leaves_a_tombstone_a_delayed_backup_cannot_undo(self):
        live = self.store.put("post:1", 17, post(1), "a")
        self.store.delete("post:1", 17, "d")
        self.assertTrue(self.store.get("post:1")["deleted"])
        self.assertEqual(self.store.import_records([live], "backup"), (0, 1))
        self.assertTrue(self.store.get("post:1")["deleted"])
        self.assertEqual(self.store.counts(), {"primary": 0, "backup": 0, "tombstones": 1})
        self.assertEqual(len(self.store.list_interval(12, 20)), 1)

    def test_interval_including_wrap_around_zero_and_single_peer_ring(self):
        for key in (30, 1, 5, 28, 4):
            self.store.put(f"post:{key}", key, post(key))
        keys = lambda a, b: sorted(r["routing_key"] for r in self.store.list_interval(a, b))
        self.assertEqual(keys(28, 4), [1, 4, 30])
        self.assertEqual(keys(4, 12), [5])
        self.assertEqual(keys(20, 20), [1, 4, 5, 28, 30])

    def test_roles_handoff_promotion_and_drop(self):
        rec = self.store.put("post:17", 17, post(17))
        self.store.set_role_for_interval(12, 18, "backup")
        self.assertEqual(self.store.get("post:17")["role"], "backup")
        self.assertEqual(self.store.import_records([rec], "primary"), (1, 0))
        self.assertEqual(self.store.get("post:17")["role"], "primary")
        self.store.set_role_for_interval(12, 18, "backup")
        self.assertEqual(self.store.promote_backups_to_primary(), 1)
        self.assertEqual(self.store.get("post:17")["role"], "primary")
        self.assertEqual(self.store.drop_interval(12, 18, "backup"), 0)
        self.assertEqual(self.store.drop_interval(12, 18, "primary"), 1)
        self.assertIsNone(self.store.get("post:17"))


if __name__ == "__main__":
    unittest.main()
