"""Integration tests that run the REAL Chord peers, Profile, Post and Feed.

    python3 -m unittest test_p2p_integration -v

Each test class starts its own processes in a fresh folder under p2p_runs/ and
stops only what it started. The tests share ports with demo_p2p.py, so do not
run both at once. Existing tests (test_chord_utils.py) are untouched.
"""

import unittest

import requests

import demo_p2p as d


class ClusterCase(unittest.TestCase):
    with_services = False

    @classmethod
    def setUpClass(cls):
        cls.cluster = d.Cluster(cls.__name__, d.DEFAULT_RUNS_DIR)
        cls.addClassCleanup(cls.cluster.stop_all)
        cls.cluster.require_free_ports(d.needed_ports(services=cls.with_services))
        cls.cluster.start_ring()
        if cls.with_services:
            cls.cluster.start_services()
        cls.client = d.ChordClient(cls.cluster.peer_url(d.ENTRY), timeout=5.0)
        cls.ring = list(d.INITIAL_RING)


class RingStorageTests(ClusterCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.posts = d.make_demo_posts()
        cls.put_problems = d.store_posts(cls.cluster, cls.client, cls.posts, cls.ring)

    def test_post_ids_are_chosen_with_the_real_hash(self):
        self.assertNotEqual(d.post_routing_key(17), 17)
        focus = d.post_id_for_key(d.FOCUS_KEY)
        self.assertEqual(d.post_routing_key(focus), d.FOCUS_KEY)
        self.assertEqual(sorted(d.post_routing_key(p) for p in self.posts),
                         sorted(d.DEMO_KEYS))

    def test_ring_links_and_finger_tables(self):
        self.assertEqual(d.ring_problems(self.cluster, self.ring), [])

    def test_puts_confirmed_primary_and_backup(self):
        self.assertEqual(self.put_problems, [])

    def test_sqlite_placement_primary_and_successor_backup(self):
        self.assertEqual(d.placement_problems(self.cluster, self.posts, self.ring), [])

    def test_lookup_from_non_owner_is_routed_and_logged(self):
        rid = d.new_request_id("test-lookup")
        status, body = d.raw_lookup(self.cluster, d.FOCUS_KEY, rid)
        self.assertEqual(d.lookup_problems(status, body, 20), [])
        self.assertGreaterEqual(body["hops"], 1)
        self.assertEqual(d.lookup_trace_problems(self.cluster, rid, body), [])

    def test_fetch_returns_exactly_what_was_stored(self):
        self.assertEqual(d.fetch_problems(self.client, self.posts), [])

    def test_missing_post_is_not_found_rather_than_an_error(self):
        key = d.post_routing_key(999999)
        self.assertIsNone(self.client.get("post:999999", key, d.new_request_id("miss")))

    def test_non_owner_refuses_record_operations(self):
        focus = d.post_id_for_key(d.FOCUS_KEY)
        reply = requests.post(self.cluster.peer_url(4) + "/records/get", timeout=5,
                              json={"record_id": f"post:{focus}",
                                    "routing_key": d.FOCUS_KEY})
        self.assertEqual(reply.status_code, 503)
        self.assertIn("does not own", reply.json()["error"])


class JoinLeaveTests(ClusterCase):
    def test_join_then_graceful_leave_moves_data_and_keeps_it_retrievable(self):
        c, ring = self.cluster, self.ring
        posts = d.make_demo_posts()
        self.assertEqual(d.store_posts(c, self.client, posts, ring), [])
        self.assertEqual(d.placement_problems(c, posts, ring), [])

        after = sorted(ring + [d.JOINER])
        c.start_peer(d.JOINER, bootstrap=d.ENTRY)
        self.assertEqual(d.ring_problems(c, after), [], "ring after join")
        status, body = d.raw_lookup(c, d.FOCUS_KEY, d.new_request_id("test-join"))
        self.assertEqual(d.lookup_problems(status, body, d.JOINER), [])
        self.assertEqual(d.placement_problems(c, posts, after), [], "data after join")
        self.assertEqual(d.fetch_problems(self.client, posts), [], "fetch after join")
        join_rid = c.request_id_of("JOIN_COMPLETE", d.JOINER)
        moved = sum(1 for p in posts if d.oracle_owner(d.post_routing_key(p), ring) == 20
                    and d.oracle_owner(d.post_routing_key(p), after) == d.JOINER)
        self.assertGreaterEqual(moved, 1, "demo posts must include data that moves")
        self.assertEqual(d.transfer_problems(c, join_rid, [
            (20, d.JOINER, "primary->primary", moved)]), [])

        leave_rid = d.new_request_id("test-leave")
        reply = requests.post(c.peer_url(d.JOINER) + "/leave", timeout=60,
                              headers={"X-Request-ID": leave_rid})
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(reply.json().get("status"), "left")
        self.assertEqual(c.wait_exit(f"peer_{d.JOINER}", 15), 0, "peer 18 exit code")
        self.assertEqual(d.ring_problems(c, ring), [], "ring after leave")
        status, body = d.raw_lookup(c, d.FOCUS_KEY, d.new_request_id("test-after"))
        self.assertEqual(d.lookup_problems(status, body, 20), [])
        self.assertEqual(
            d.placement_problems(c, posts, ring, also_empty=[d.JOINER]), [],
            "data after leave")
        self.assertEqual(d.fetch_problems(self.client, posts), [], "fetch after leave")
        owned = d.count_owned(posts, after, d.JOINER)
        self.assertEqual(d.transfer_problems(c, leave_rid, [
            (d.JOINER, 20, "primary->primary", owned),
            (d.JOINER, 28, "primary->backup", owned)]), [])

    def test_joining_with_a_live_peer_id_is_rejected(self):
        c = self.cluster
        reply = requests.post(c.peer_url(12) + "/join", timeout=30,
                              json={"bootstrap_url": c.peer_url(d.ENTRY)})
        self.assertEqual(reply.status_code, 409, reply.text)
        self.assertEqual(d.ring_problems(c, self.ring), [], "ring must be unchanged")


class CrashRecoveryTests(ClusterCase):
    def test_crash_of_peer_20_promotes_backups_and_restores_backup_placement(self):
        c = self.cluster
        posts = d.make_demo_posts()
        self.assertEqual(d.store_posts(c, self.client, posts, self.ring), [])
        self.assertEqual(d.placement_problems(c, posts, self.ring), [], "backups exist")
        held = [p for p, row in c.db_rows(20).items() if row["role"] == "primary"]
        self.assertTrue(held, "peer 20 must hold data for the crash to matter")

        c.kill("peer_20")
        c.wait_until("peer 28 recovery",
                     lambda: any(e.event == "RECOVERY_COMPLETE" for e in c.entries(28)),
                     d.RECOVERY_SECONDS, "peer_28")
        survivors = [4, 12, 28]
        self.assertEqual(d.ring_problems(c, survivors), [], "ring after crash")
        self.assertEqual(d.placement_problems(c, posts, survivors), [],
                         "promotion and restored backups")
        status, body = d.raw_lookup(c, d.FOCUS_KEY, d.new_request_id("test-crash"))
        self.assertEqual(d.lookup_problems(status, body, 28), [])
        self.assertEqual(d.fetch_problems(self.client, posts), [], "fetch after crash")
        extra = d.make_demo_posts(keys=(9,), start=d.DEMO_ID_START + 500)
        self.assertEqual(d.store_posts(c, self.client, extra, survivors), [],
                         "writes work after recovery")


class PostFeedTests(ClusterCase):
    """Need Post to store its records on the peers. See the 501 note below."""

    with_services = True

    def test_post_creation_over_real_peers(self):
        created, failures = d.create_posts_via_post(self.cluster)
        self.assertEqual(failures, [], "Post could not create posts over the peers")
        self.assertEqual(len(created), 3)

    def test_retrieve_and_feed_survive_join_and_leave(self):
        c = self.cluster
        created, failures = d.create_posts_via_post(c)
        self.assertEqual(failures, [], "Post could not create posts over the peers")
        self.assertEqual(d.post_feed_problems(created), [], "baseline")
        c.start_peer(d.JOINER, bootstrap=d.ENTRY)
        self.assertEqual(d.post_feed_problems(created), [], "after join")
        reply = requests.post(c.peer_url(d.JOINER) + "/leave", timeout=60)
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(c.wait_exit(f"peer_{d.JOINER}", 15), 0)
        self.assertEqual(d.post_feed_problems(created), [], "after leave")
        self.assertEqual(
            d.placement_problems(c, created, self.ring, also_empty=[d.JOINER]), [])


    def test_deleted_post_is_not_restored_by_join_or_leave(self):
        c = self.cluster
        created, failures = d.create_posts_via_post(c)
        self.assertEqual(failures, [], "Post could not create posts over the peers")
        gone_id = sorted(created)[0]
        gone = created.pop(gone_id)
        reply = requests.delete(f"{d.POST_URL}/posts/{gone_id}", timeout=20)
        self.assertEqual(reply.status_code, 200, reply.text)
        key = d.post_routing_key(gone_id)
        for stage in ("after delete", "after join", "after leave"):
            if stage == "after join":
                c.start_peer(d.JOINER, bootstrap=d.ENTRY)
            if stage == "after leave":
                requests.post(c.peer_url(d.JOINER) + "/leave", timeout=60)
                self.assertEqual(c.wait_exit(f"peer_{d.JOINER}", 15), 0)
            self.assertIsNone(self.client.get(f"post:{gone_id}", key,
                                              d.new_request_id("deleted")), stage)
            self.assertEqual(d.post_feed_problems(created), [], stage)
            listed = requests.post(d.POST_URL + "/posts/retrieve", timeout=20,
                                   json={"author": gone["author"]}).json()["posts"]
            self.assertNotIn(gone_id, [p["id"] for p in listed], stage)
            for peer_id, table in ((p, c.db_rows(p)) for p in [4, 12, 20, 28]):
                self.assertNotIn(gone_id, table, f"{stage}: peer {peer_id} still holds it")


if __name__ == "__main__":
    unittest.main()
