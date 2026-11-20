"""Tests for coordination_state.py and coordinator_server.py (Member 3).

    pytest test_coordinator.py -v

Part 1 (pure state machine) always runs.
Part 2 starts three REAL coordinator servers (coord-1/2/3) on free ports with
Member 1's MessageContext and Member 2's ZKCoordination, against a real
ZooKeeper (ZK_HOSTS, default 127.0.0.1:2181). It skips if ZooKeeper is down:
    docker compose -f compose.zookeeper.yml up -d
"""
import json
import os
import socket
import threading
import time
import uuid

import pytest
import requests

import coordination_state as cs

HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181")


def info(n=1):
    return {"node_id": "coord-1:test", "coordinator_generation": 7,
            "vector_clock": {"coord-1:test": n}}


def acquire(state, owner, rid, n=1):
    new, res = cs.apply_acquire(state, owner, rid, info(n))
    return (new if new is not None else state), res


def release(state, owner, rid, token, n=1):
    new, res = cs.apply_release(state, owner, rid, token, info(n))
    return (new if new is not None else state), res


# =========================================================== part 1: pure

class TestSchema:
    def test_initial_state_is_valid_and_empty(self):
        s = cs.initial_state()
        cs.validate_state(s)
        assert s["holder"] is None and s["queue"] == [] and s["next_token"] == 1
        assert s["requests"] == {} and s["vector_clock"] == {} and s["history"] == []
        assert json.loads(json.dumps(s)) == s                 # JSON-serialisable

    def test_initial_state_returns_independent_copies(self):
        a = cs.initial_state()
        a["queue"].append("x")
        assert cs.initial_state()["queue"] == []

    def test_validate_rejects_broken_invariants(self):
        s, g1 = acquire(cs.initial_state(), "a", "r1")
        s, _ = acquire(s, "b", "r2")
        two = json.loads(json.dumps(s))
        two["requests"]["b r2"]["status"] = "granted"
        two["requests"]["b r2"]["grant_token"] = 1
        with pytest.raises(cs.StateError):
            cs.validate_state(two)                            # two granted requests
        dup = json.loads(json.dumps(s))
        dup["queue"].append(dict(dup["queue"][0]))
        with pytest.raises(cs.StateError):
            cs.validate_state(dup)                            # queued twice
        orphan = json.loads(json.dumps(s))
        orphan["holder"] = None
        orphan["requests"]["a r1"]["status"] = "released"
        with pytest.raises(cs.StateError):
            cs.validate_state(orphan)                         # waiters but no holder

    def test_bad_ids_rejected(self):
        for owner, rid in (("", "r"), ("a b", "r"), ("a", None), ("a", "x" * 301)):
            with pytest.raises(ValueError):
                cs.request_key(owner, rid)


class TestGrantMatches:
    def test_only_current_holder_matches(self):
        s, res = acquire(cs.initial_state(), "a", "r1")
        grant = res["grant"]
        assert cs.grant_matches(s, grant)
        assert not cs.grant_matches(s, dict(grant, grant_token=grant["grant_token"] + 1))
        assert not cs.grant_matches(s, dict(grant, owner_id="b"))
        assert not cs.grant_matches(s, dict(grant, request_id="other"))
        assert not cs.grant_matches(s, dict(grant, grant_token=True))
        assert not cs.grant_matches(s, None) and not cs.grant_matches(None, grant)
        s, _ = release(s, "a", "r1", grant["grant_token"])
        assert not cs.grant_matches(s, grant)                 # released grant is dead


class TestMutualExclusion:
    def test_only_first_request_is_granted(self):
        s = cs.initial_state()
        outcomes = []
        for i in range(6):
            s, res = acquire(s, f"post-{i}", f"r{i}")
            outcomes.append(res["outcome"])
            cs.validate_state(s)
        assert outcomes == ["granted"] + ["queued"] * 5
        assert sum(r["status"] == "granted" for r in s["requests"].values()) == 1

    def test_fifo_handoff_in_arrival_order_with_increasing_tokens(self):
        s = cs.initial_state()
        for i in range(4):
            s, _ = acquire(s, f"p{i}", f"r{i}")
        order, tokens = [], []
        for _ in range(4):
            h = s["holder"]
            order.append(h["owner_id"])
            tokens.append(h["grant_token"])
            s, res = release(s, h["owner_id"], h["request_id"], h["grant_token"])
            assert res["outcome"] == "released"
            cs.validate_state(s)
        assert order == ["p0", "p1", "p2", "p3"]
        assert tokens == [1, 2, 3, 4]
        assert s["holder"] is None and s["queue"] == [] and s["next_token"] == 5

    def test_release_grants_next_in_same_transition(self):
        s, g = acquire(cs.initial_state(), "a", "r1")
        s, _ = acquire(s, "b", "r2")
        s, res = release(s, "a", "r1", g["grant"]["grant_token"])
        assert res["next_grant"]["owner_id"] == "b" and s["holder"]["owner_id"] == "b"


class TestIdempotency:
    def test_duplicate_acquire_never_enqueues_twice_or_regrants(self):
        s, g = acquire(cs.initial_state(), "a", "r1")
        s, q = acquire(s, "b", "r2")
        for _ in range(3):
            new, res = cs.apply_acquire(s, "a", "r1", info())
            assert new is None and res["grant"] == g["grant"] and res["duplicate"]
            new, res = cs.apply_acquire(s, "b", "r2", info())
            assert new is None and res["outcome"] == "queued" and res["position"] == 1
        assert len(s["queue"]) == 1 and s["next_token"] == 2

    def test_duplicate_release_is_harmless(self):
        s, g = acquire(cs.initial_state(), "a", "r1")
        tok = g["grant"]["grant_token"]
        s, first = release(s, "a", "r1", tok)
        new, again = cs.apply_release(s, "a", "r1", tok, info())
        assert first["outcome"] == "released" and again["outcome"] == "duplicate"
        assert new is None

    def test_finished_request_id_cannot_be_reacquired(self):
        s, g = acquire(cs.initial_state(), "a", "r1")
        s, _ = release(s, "a", "r1", g["grant"]["grant_token"])
        new, res = cs.apply_acquire(s, "a", "r1", info())
        assert new is None and res["outcome"] == "finished" and res["status"] == "released"


class TestStaleRelease:
    def test_wrong_token_unknown_request_and_queued_with_token(self):
        s, g = acquire(cs.initial_state(), "a", "r1")
        s, _ = acquire(s, "b", "r2")
        tok = g["grant"]["grant_token"]
        for owner, rid, token in (("a", "r1", tok + 5), ("a", "r1", None), ("zz", "r9", tok),
                                  ("b", "r2", tok), ("b", "r1", tok)):
            new, res = cs.apply_release(s, owner, rid, token, info())
            assert new is None and res["outcome"] == "stale", (owner, rid, token)
        assert s["holder"]["owner_id"] == "a"

    def test_old_holder_cannot_release_the_new_holders_grant(self):
        s, g = acquire(cs.initial_state(), "a", "r1")
        s, _ = acquire(s, "b", "r2")
        s, _ = release(s, "a", "r1", g["grant"]["grant_token"])
        new, res = cs.apply_release(s, "a", "r1", s["holder"]["grant_token"], info())
        assert new is None and res["outcome"] == "stale" and s["holder"]["owner_id"] == "b"

    def test_cancel_queued_request(self):
        s, _ = acquire(cs.initial_state(), "a", "r1")
        s, _ = acquire(s, "b", "r2")
        s, _ = acquire(s, "c", "r3")
        s, res = release(s, "b", "r2", None)
        assert res["outcome"] == "cancelled"
        assert [e["owner_id"] for e in s["queue"]] == ["c"]
        cs.validate_state(s)


class TestHistoryAndPruning:
    def test_vector_history_merges_and_is_bounded(self):
        s = cs.initial_state()
        for i in range(cs.MAX_HISTORY + 20):
            s, res = acquire(s, f"p{i}", "r", n=i + 1)
            if res["outcome"] == "granted":
                s, _ = release(s, f"p{i}", "r", res["grant"]["grant_token"], n=i + 1)
        assert len(s["history"]) == cs.MAX_HISTORY
        assert s["vector_clock"] == {"coord-1:test": cs.MAX_HISTORY + 20}
        assert s["updated_by"] == {"node_id": "coord-1:test", "coordinator_generation": 7}

    def test_completed_requests_are_pruned_but_live_ones_kept(self):
        s, held = acquire(cs.initial_state(), "keeper", "r")
        s, _ = acquire(s, "waiter", "r")
        for i in range(cs.MAX_COMPLETED + 10):              # queued then cancelled
            s, _ = acquire(s, f"x{i}", "r")
            s, _ = release(s, f"x{i}", "r", None)
        cs.validate_state(s)
        assert len(s["completed"]) == cs.MAX_COMPLETED
        assert "keeper r" in s["requests"] and "waiter r" in s["requests"]


# ================================================ part 2: real ZooKeeper

kazoo = pytest.importorskip("kazoo")
from kazoo.client import KazooClient                                  # noqa: E402
from kazoo.exceptions import ConnectionLoss                           # noqa: E402
from werkzeug.serving import make_server                              # noqa: E402

from coordinator_server import LockCoordinator, create_app            # noqa: E402
from message_protocol import MessageContext                           # noqa: E402
from zk_coordination import ZKCoordination, decode_state              # noqa: E402


def wait_for(pred, timeout=20.0, step=0.1):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def expire_session(zk):
    """Kill the ZooKeeper session server-side: same as the coordinator crashing."""
    sid, pw = zk.client.client_id
    other = KazooClient(hosts=HOSTS, client_id=(sid, pw))
    other.start()
    other.stop()
    other.close()


class Node:
    """One real coordinator candidate: ctx + ZKCoordination + Flask HTTP server."""

    def __init__(self, name, root, log_dir):
        self.name = name
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.ctx = MessageContext(name, log_path=str(log_dir / f"{name}.clock.jsonl"))
        self.zk = ZKCoordination(self.ctx.node_id, self.ctx, hosts=HOSTS, root=root,
                                 advertised_url=self.url, session_timeout=6.0)
        self.coord = LockCoordinator(self.ctx, self.zk)
        self.server = make_server("127.0.0.1", self.port, create_app(self.coord), threaded=True)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.zk.start()
        self.coord.start_recovery_watcher()

    def events(self, name=None):
        if not os.path.exists(self.ctx.log_path):
            return []
        with open(self.ctx.log_path, encoding="utf-8") as fh:
            recs = [json.loads(line) for line in fh]
        return [r for r in recs if name is None or r["event"] == name]

    def close(self):
        self.coord.stop()
        self.server.shutdown()
        self.zk.close()


class Client:
    """A lock client (like Post) using Member 1's headers on every message."""

    def __init__(self, name, log_dir):
        self.ctx = MessageContext(name, log_path=str(log_dir / f"{name}.clock.jsonl"))
        self.owner = self.ctx.node_id

    def call(self, node, path, body, request_id=None, method="POST"):
        rid = request_id or str(uuid.uuid4())
        headers = self.ctx.send_headers(rid)
        resp = requests.request(method, node.url + path, json=body, headers=headers, timeout=15)
        self.ctx.receive_headers(resp.headers)
        return resp.status_code, resp.json()

    def acquire(self, node, request_id):
        return self.call(node, "/lock/acquire", {"resource": "post-write",
                         "owner_id": self.owner, "request_id": request_id}, request_id)

    def release(self, node, request_id, token):
        return self.call(node, "/lock/release", {"resource": "post-write", "owner_id": self.owner,
                         "request_id": request_id, "grant_token": token}, request_id)


@pytest.fixture(scope="module")
def raw_zk():
    c = KazooClient(hosts=HOSTS)
    try:
        c.start(timeout=3)
    except Exception:
        pytest.skip(f"ZooKeeper not reachable at {HOSTS}")
    yield c
    c.stop()
    c.close()


@pytest.fixture
def cluster(raw_zk, tmp_path):
    root = f"/test-coord-{uuid.uuid4().hex[:8]}/m4"
    nodes = []

    def start(name):
        n = Node(name, root, tmp_path)
        nodes.append(n)
        return n

    c1 = start("coord-1")
    assert wait_for(c1.zk.is_leader)
    c2, c3 = start("coord-2"), start("coord-3")
    assert wait_for(lambda: c1.coord._recovered_generation == c1.zk.generation)
    env = type("Env", (), {})()
    env.nodes, env.root, env.tmp, env.zk = [c1, c2, c3], root, tmp_path, raw_zk
    env.client = lambda name: Client(name, tmp_path)
    env.state = lambda: decode_state(raw_zk.get(f"{root}/locks/post-write/state")[0])
    yield env
    for n in nodes:
        try:
            n.close()
        except Exception:
            pass
    try:
        raw_zk.delete(root.rsplit("/", 1)[0], recursive=True)
    except Exception:
        pass


def test_concurrent_acquires_grant_exactly_one_then_fifo(cluster):
    leader = cluster.nodes[0]
    clients = [cluster.client(f"post-{i}") for i in range(8)]
    results = [None] * 8

    def go(i):
        results[i] = clients[i].acquire(leader, f"req-{i}")

    threads = [threading.Thread(target=go, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    granted = [i for i, (code, _) in enumerate(results) if code == 200]
    assert len(granted) == 1 and sum(code == 202 for code, _ in results) == 7
    state = cluster.state()
    cs.validate_state(state)
    queue_order = [e["owner_id"] for e in state["queue"]]
    # release one by one: holders must follow the stored FIFO queue exactly
    seen = []
    by_owner = {c.owner: (i, c) for i, c in enumerate(clients)}
    for _ in range(8):
        h = cluster.state()["holder"]
        seen.append(h["owner_id"])
        i, c = by_owner[h["owner_id"]]
        code, body = c.acquire(leader, f"req-{i}")              # poll returns the grant
        assert code == 200 and body["grant"]["grant_token"] == h["grant_token"]
        code, body = c.release(leader, f"req-{i}", h["grant_token"])
        assert code == 200 and body["status"] == "released"
    assert seen[1:] == queue_order
    final = cluster.state()
    assert final["holder"] is None and final["next_token"] == 9


def test_duplicate_acquire_and_release_over_http(cluster):
    leader = cluster.nodes[0]
    a, b = cluster.client("post-a"), cluster.client("post-b")
    code, g = a.acquire(leader, "op-1")
    assert code == 200 and not g["duplicate"]
    code, g2 = a.acquire(leader, "op-1")                         # retry
    assert code == 200 and g2["duplicate"] and g2["grant"] == g["grant"]
    assert b.acquire(leader, "op-2")[0] == 202
    assert b.acquire(leader, "op-2")[1]["position"] == 1         # still once in queue
    st = cluster.state()
    assert len(st["queue"]) == 1 and st["next_token"] == 2
    tok = g["grant"]["grant_token"]
    code, r1 = a.release(leader, "op-1", tok)
    code2, r2 = a.release(leader, "op-1", tok)                   # duplicate release
    assert (code, r1["duplicate"], r1["next_grant"]["owner_id"]) == (200, False, b.owner)
    assert (code2, r2["duplicate"]) == (200, True)
    assert cluster.state()["holder"]["owner_id"] == b.owner      # not released twice
    assert a.acquire(leader, "op-1")[0] == 409                   # finished id not reusable


def test_stale_release_is_rejected(cluster):
    leader = cluster.nodes[0]
    a, b = cluster.client("post-a"), cluster.client("post-b")
    _, g = a.acquire(leader, "op-1")
    tok = g["grant"]["grant_token"]
    b.acquire(leader, "op-2")
    code, body = a.release(leader, "op-1", tok + 99)
    assert code == 409 and body["error"] == "stale_release"
    code, body = b.release(leader, "op-1", tok)                  # someone else's request
    assert code == 409
    a.release(leader, "op-1", tok)
    new_tok = cluster.state()["holder"]["grant_token"]
    code, _ = a.release(leader, "op-1", new_tok)                 # old holder, new token
    assert code == 409 and cluster.state()["holder"]["owner_id"] == b.owner
    assert leader.events("LOCK_RELEASE_STALE")


def test_non_leaders_reject_changes_and_point_to_leader(cluster):
    c1, c2, c3 = cluster.nodes
    a = cluster.client("post-a")
    before = cluster.state()
    for node in (c2, c3):
        code, body = a.acquire(node, "op-x")
        assert code == 503 and body["error"] == "not_leader"
        assert body["leader"]["url"] == c1.url
        code, body = a.release(node, "op-x", 1)
        assert code == 503
    assert cluster.state() == before                             # nothing written
    code, status = a.call(c2, "/lock/status", None, method="GET")
    assert code == 200 and not status["is_leader"] and status["leader"]["url"] == c1.url
    assert c2.events("LOCK_REJECTED_NOT_LEADER")


def test_recovery_keeps_outstanding_grant_queue_tokens_and_vectors(cluster):
    c1, c2, c3 = cluster.nodes
    a, b, c = cluster.client("post-a"), cluster.client("post-b"), cluster.client("post-c")
    _, ga = a.acquire(c1, "op-a")
    assert b.acquire(c1, "op-b")[0] == 202
    assert c.acquire(c1, "op-c")[0] == 202
    before = cluster.state()
    old_vector = before["vector_clock"]
    assert c1.ctx.node_id in old_vector

    expire_session(c1.zk)                                        # leader "crashes"
    assert wait_for(c2.zk.is_leader)
    assert wait_for(lambda: c2.coord._recovered_generation == c2.zk.generation)
    after = cluster.state()
    assert after["holder"] == before["holder"]                   # grant survives
    assert after["queue"] == before["queue"]
    assert after["next_token"] == before["next_token"]
    assert after["requests"] == before["requests"]

    rec = c2.events("STATE_RECOVERED")[-1]
    assert rec["holder"]["owner_id"] == a.owner and rec["queue_length"] == 2
    assert rec["previous_coordinator"] == c1.ctx.node_id
    for node, count in old_vector.items():                      # merged vector history
        assert c2.ctx.clock.snapshot().get(node, 0) >= count

    # the deposed leader can no longer change anything
    assert a.acquire(c1, "op-a")[0] == 503
    # polls on the new leader are idempotent; the old grant is still valid
    code, g = a.acquire(c2, "op-a")
    assert code == 200 and g["grant"]["grant_token"] == ga["grant"]["grant_token"]
    assert b.acquire(c2, "op-b")[1]["position"] == 1
    code, body = a.release(c2, "op-a", ga["grant"]["grant_token"])
    assert code == 200 and body["next_grant"]["owner_id"] == b.owner
    assert body["next_grant"]["grant_token"] == before["next_token"]   # monotonic
    final = cluster.state()
    cs.validate_state(final)
    assert final["updated_by"]["node_id"] == c2.ctx.node_id
    assert final["history"][-1]["coordinator_generation"] == c2.zk.generation


def test_ambiguous_commit_is_reread_not_regranted(cluster):
    leader = cluster.nodes[0]
    real_txn = leader.zk.leader_txn
    calls = {"n": 0}

    def flaky_txn():
        txn = real_txn()
        real_commit = txn.commit

        def commit():
            calls["n"] += 1
            if calls["n"] == 1:
                real_commit()                    # it DID apply ...
                raise ConnectionLoss()           # ... but the reply was lost
            return real_commit()
        txn.commit = commit
        return txn

    leader.zk.leader_txn = flaky_txn
    a = cluster.client("post-a")
    code, body = a.acquire(leader, "op-1")
    assert code == 200 and body["grant"]["grant_token"] == 1 and not body["duplicate"]
    st = cluster.state()
    assert st["next_token"] == 2                                 # one token, not two
    assert list(st["requests"]) == [f"{a.owner} op-1"]
    assert leader.events("LOCK_COMMIT_UNKNOWN")
    calls["n"] = 0
    code, body = a.release(leader, "op-1", 1)                   # same for release
    assert code == 200 and body["status"] == "released" and not body["duplicate"]
    assert cluster.state()["holder"] is None


def test_grant_is_persisted_before_it_is_returned(cluster):
    leader = cluster.nodes[0]
    real_txn = leader.zk.leader_txn

    def failing_txn():                         # commit never succeeds
        txn = real_txn()
        txn.commit = lambda: [Exception("check failed"), Exception("rolled back")]
        return txn

    leader.zk.leader_txn = failing_txn
    a = cluster.client("post-a")
    code, body = a.acquire(leader, "op-1")
    assert code == 503 and "grant" not in body                   # no unpersisted grant
    assert cluster.state()["holder"] is None
    leader.zk.leader_txn = real_txn
    code, body = a.acquire(leader, "op-1")
    assert code == 200 and cs.grant_matches(cluster.state(), body["grant"])


def test_restart_does_not_clear_state_and_grant_never_expires(cluster):
    c1 = cluster.nodes[0]
    a = cluster.client("post-a")
    _, g = a.acquire(c1, "op-1")
    for n in cluster.nodes:                                      # every coordinator stops
        n.close()
    time.sleep(1.0)
    fresh = Node("coord-1", cluster.root, cluster.tmp)           # cold start
    cluster.nodes.append(fresh)
    assert wait_for(fresh.zk.is_leader)
    st = cluster.state()
    assert cs.grant_matches(st, g["grant"])                      # still held, no expiry
    code, body = a.acquire(fresh, "op-1")
    assert code == 200 and body["grant"] == g["grant"]
