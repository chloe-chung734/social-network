"""tests for zk_coordination.py

needs a real ZooKeeper running:
    docker compose -f compose.zookeeper.yml up -d
    pytest test_zk_coordination.py -v

tests skip themselves if ZooKeeper isn't there (ZK_HOSTS, default 127.0.0.1:2181)
"""
import json
import os
import time
import uuid

import pytest
from kazoo.client import KazooClient

from message_protocol import MessageContext
from zk_coordination import (
    NotLeaderError, ZKCoordination, decode_state, encode_state,
)

HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181")


def state_factory():
    try:
        from coordination_state import initial_state
        return initial_state()
    except ImportError:  # Member 3's file isn't in yet, use a fake one
        return {"holder": None, "next_token": 1, "queue": [], "requests": {}}


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


def expire_session(coord):
    """Kills the session on the server side, same as the coordinator crashing."""
    sid, pw = coord.client.client_id
    other = KazooClient(hosts=HOSTS, client_id=(sid, pw))
    other.start()
    other.stop()
    other.close()


@pytest.fixture(scope="module")
def raw_client():
    c = KazooClient(hosts=HOSTS)
    try:
        c.start(timeout=3)
    except Exception:
        pytest.skip(f"ZooKeeper not reachable at {HOSTS}")
    yield c
    c.stop()
    c.close()


@pytest.fixture
def root(raw_client):
    r = f"/test-{uuid.uuid4().hex[:8]}/m4"
    yield r
    try:
        raw_client.delete(r.rsplit("/", 1)[0], recursive=True)
    except Exception:
        pass


@pytest.fixture
def make(root, tmp_path):
    created, ctxs = [], []

    def _make(name, url=None):
        ctx = MessageContext(name, log_path=str(tmp_path / f"{name}.clock.jsonl"))
        z = ZKCoordination(
            ctx.node_id, ctx, hosts=HOSTS, root=root, advertised_url=url,
            session_timeout=6.0, state_factory=state_factory,
        )
        z.start()
        created.append(z)
        ctxs.append(ctx)
        return z

    def events():
        out = []
        for ctx in ctxs:
            if os.path.exists(ctx.log_path):
                with open(ctx.log_path, encoding="utf-8") as fh:
                    out += [json.loads(line) for line in fh]
        return out

    _make.events = events
    yield _make
    for z in created:
        z.close()


def three(make):
    c1 = make("coord-1", "http://127.0.0.1:5101")
    assert wait_for(c1.is_leader)
    c2 = make("coord-2", "http://127.0.0.1:5102")
    c3 = make("coord-3", "http://127.0.0.1:5103")
    return c1, c2, c3


def test_three_candidates_one_winner(make):
    c1, c2, c3 = three(make)
    assert wait_for(lambda: [z.is_leader() for z in (c1, c2, c3)] == [True, False, False])
    assert c1.generation < c2.generation < c3.generation
    recs = make.events()
    names = [r["event"] for r in recs]
    assert names.count("ELECTION_JOIN") == 3 and names.count("LEADER_READY") == 1
    for r in recs:  # every log line needs these fields
        assert {"node_id", "request_id", "event", "vector_clock"} <= set(r)
        assert r["vector_clock"][r["node_id"]] >= 1
    ready = [r for r in recs if r["event"] == "LEADER_READY"][0]
    assert ready["coordinator_generation"] == c1.generation


def test_discovery_only_is_not_a_candidate(make, raw_client, root):
    leader = make("coord-1", "http://127.0.0.1:5101")
    assert wait_for(leader.is_leader)
    post = make("post-a")  # no advertised_url
    assert post.candidate_path is None and post.generation is None
    assert not post.is_leader()
    assert len(raw_client.get_children(f"{root}/election")) == 1
    assert set(post.members()) == {leader.node_id, post.node_id}
    assert wait_for(lambda: (post.leader_info() or {}).get("node_id") == leader.node_id)
    info = post.leader_info()
    assert info["url"] == "http://127.0.0.1:5101" and info["ready"]
    assert set(info) >= {"node_id", "url", "candidate_path", "generation", "vector_clock"}


def test_no_ready_leader_before_any_candidate(make):
    assert make("post-a").leader_info() is None


def test_crash_replacement_and_discovery(make):
    c1, c2, c3 = three(make)
    post = make("post-a")
    assert wait_for(lambda: (post.leader_info() or {}).get("node_id") == c1.node_id)
    old_gen = c1.generation
    expire_session(c1)
    assert wait_for(lambda: not c1.is_leader())
    assert wait_for(c2.is_leader)               # c2 was next in line
    assert not c3.is_leader()
    assert wait_for(lambda: (post.leader_info() or {}).get("node_id") == c2.node_id)
    assert post.leader_info()["generation"] > old_gen
    # the crashed one comes back on a new session and goes to the back
    assert wait_for(lambda: c1.generation is not None and c1.generation > c3.generation)
    assert not c1.is_leader()
    lost = [r for r in make.events() if r["event"] == "LEADERSHIP_LOST"]
    assert lost and lost[0]["node_id"] == c1.node_id


def test_middle_candidate_loss_rewatches(make):
    c1, c2, c3 = three(make)
    expire_session(c2)
    assert wait_for(lambda: c2.generation is not None and c2.generation > c3.generation)
    assert c1.is_leader() and not c3.is_leader()
    expire_session(c1)
    assert wait_for(c3.is_leader)  # c3 had to skip over the dead c2


def test_existing_lock_state_never_reset(make):
    c1, c2, _ = three(make)
    state, v = c1.read_lock_state()
    state["marker"] = "outstanding-grant"
    c1.client.set(c1.lock_state_path, encode_state(state), version=v)
    expire_session(c1)
    assert wait_for(c2.is_leader)
    assert c2.read_lock_state()[0]["marker"] == "outstanding-grant"


def test_leader_txn_fences_deposed_leader(make):
    c1, c2, _ = three(make)
    tok = c1.leadership_token()
    expire_session(c1)
    assert wait_for(c2.is_leader)
    with pytest.raises(NotLeaderError):
        c1.leader_txn()
    before, v = c2.read_lock_state()
    txn = c2.client.transaction()
    txn.check(tok["candidate_path"], tok["version"])
    txn.set_data(c2.lock_state_path, encode_state({"hijack": True}))
    assert any(isinstance(r, Exception) for r in txn.commit())
    assert c2.read_lock_state()[0] == before
    txn = c2.leader_txn()
    txn.set_data(c2.lock_state_path, encode_state(dict(before, ok=1)), version=v)
    assert not any(isinstance(r, Exception) for r in txn.commit())
    assert c2.read_lock_state()[0]["ok"] == 1


def test_advertisement_is_ephemeral_and_replaced(make, raw_client, root):
    c1, c2, _ = three(make)
    raw, _ = raw_client.get(f"{root}/coordinator")
    assert decode_state(raw)["node_id"] == c1.node_id
    expire_session(c1)
    assert wait_for(lambda: decode_state(raw_client.get(f"{root}/coordinator")[0])["node_id"] == c2.node_id)
