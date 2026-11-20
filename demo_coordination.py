"""Member 7's real-process Milestone 4 demo. See MILESTONE_4_README.md.

No replacement clock, coordinator, or storage implementation lives here.
Missing teammates' components are BLOCKED, never a successful demo.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import sys
import time
import uuid
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent
COMPONENTS = ("message_protocol", "vector_clock", "zk_coordination",
              "coordination_state", "coordination_client", "coordinator_server", "kazoo")
PORTS = (5001, 5002, 5003, 5004, 5005, 5006, 5101, 5102, 5103,
         6003, 7004, 7012, 7020, 7028)


class DemoFailure(RuntimeError):
    pass


def preflight():
    missing = [name for name in COMPONENTS if importlib.util.find_spec(name) is None]
    if missing:
        raise DemoFailure("BLOCKED: missing " + ", ".join(missing)
                          + ". Merge teammates' modules/install dependencies first.")


def require(condition, message):
    if not condition:
        raise DemoFailure(message)


def wait_for(label, predicate, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise DemoFailure(f"Timed out waiting for {label}")


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def happens_before(a, b):
    """An independent test oracle; this is not a production clock."""
    keys = a.keys() | b.keys()
    return (all(a.get(k, 0) <= b.get(k, 0) for k in keys)
            and any(a.get(k, 0) < b.get(k, 0) for k in keys))


def read_events(directory):
    events = []
    for path in Path(directory).rglob("*.jsonl"):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except ValueError as error:
                raise DemoFailure(f"Invalid JSONL at {path}:{number}") from error
            if all(key in item for key in ("node_id", "request_id", "event", "vector_clock")):
                item = dict(item, source=f"{path.name}:{number}")
                events.append(item)
    return events


def check_critical_sections(events, request_ids):
    """Prove ordering from vectors, never filesystem order or wall clocks."""
    sections = []
    for rid in request_ids:
        enters = [e for e in events if e["request_id"] == rid and e["event"] == "CS_ENTER"]
        exits = [e for e in events if e["request_id"] == rid and e["event"] == "CS_EXIT"]
        require(len(enters) == len(exits) == 1, f"Need exactly one CS_ENTER/CS_EXIT for {rid}")
        enter, leave = enters[0], exits[0]
        require(enter["node_id"] == leave["node_id"], f"Different CS owners for {rid}")
        require(happens_before(enter["vector_clock"], leave["vector_clock"]),
                f"CS_EXIT does not follow CS_ENTER for {rid}")
        require(enter.get("grant_token") == leave.get("grant_token")
                and type(enter.get("grant_token")) is int, f"Missing/mismatched grant token for {rid}")
        sections.append((enter, leave))
    for i, (a, ae) in enumerate(sections):
        for b, be in sections[i + 1:]:
            require(a["grant_token"] != b["grant_token"], "Two operations reused a grant token")
            require(happens_before(ae["vector_clock"], b["vector_clock"])
                    or happens_before(be["vector_clock"], a["vector_clock"]),
                    f"Cannot prove exclusive sections for {a['request_id']} and {b['request_id']}")
    return sections


def check_concurrent_origins(events, request_ids):
    origins = []
    for rid in request_ids:
        sends = [e for e in events if e["request_id"] == rid and e["event"] == "HTTP_SEND"
                 and e["node_id"].startswith("client-") and "/likes" in e.get("url", "")]
        require(len(sends) == 1, f"Need one original client send for {rid}")
        origins.append(sends[0]["vector_clock"])
    a, b = origins
    require(a != b and not happens_before(a, b) and not happens_before(b, a),
            "Independent client request origins were not concurrent")


class Wire:
    """Timestamped test-controller traffic using the real shared module."""

    def __init__(self, context):
        self.context = context
        self.exchanges = []

    def call(self, method, url, body=None, request_id=None, timeout=5):
        rid = request_id or str(uuid.uuid4())
        headers = self.context.send_headers(rid, extra={"Content-Type": "application/json"})
        sent = json.loads(headers["X-Vector-Clock"])
        response = requests.request(method, url, json=body, headers=headers, timeout=timeout)
        require(response.headers.get("X-Node-ID") and response.headers.get("X-Vector-Clock"),
                f"Untracked application response: {url}")
        remote = json.loads(response.headers["X-Vector-Clock"])
        require(response.headers.get("X-Request-ID") == rid, f"Lost request ID at {url}")
        self.context.receive_headers(response.headers)
        received = self.context.clock.snapshot()
        require(happens_before(sent, remote) and happens_before(remote, received),
                f"Request/reply causal clock violation at {url}")
        self.exchanges.append({"url": url, "request_id": rid, "sent": sent,
                               "reply": remote, "received": received})
        return response.status_code, response.json()

    def ok(self, method, url, body=None, request_id=None):
        code, data = self.call(method, url, body, request_id)
        require(code in (200, 201) and data.get("success") is not False,
                f"{method} {url}: HTTP {code}: {data}")
        return data


def discovery(context):
    from zk_coordination import ZKCoordination
    service = ZKCoordination(context.node_id, context,
                             hosts=os.environ["ZK_HOSTS"], root=os.environ["COORD_ROOT"])
    service.start()
    return service


def leader_lock_call(service, wire, path, body, rid):
    """Retry only idempotent lock requests, not application writes."""
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        leader = service.leader_info()
        if leader:
            try:
                code, data = wire.call("POST", leader["url"] + path, body, rid)
                if code == 200:
                    return data
                require(code in (202, 503), f"Lock protocol rejected request: {code} {data}")
            except requests.RequestException:
                pass  # The same request/grant can safely be retried after rediscovery.
        time.sleep(0.2)
    raise DemoFailure("Lock request timed out; ownership has NOT been cleared")


def holder_worker(args):
    """A real storage writer paused at a filesystem test barrier, not a mock.

    No network/storage request is held open at the barrier. This avoids
    deliberately triggering Post's short Chord RPC timeout during election.
    """
    from message_protocol import MessageContext
    context = MessageContext(os.environ["NODE_NAME"])
    wire, service = Wire(context), discovery(context)
    grant = None
    try:
        answer = leader_lock_call(service, wire, "/locks/acquire",
                                  {"resource": "post-write", "request_id": args.rid}, args.rid)
        grant = answer["grant"]
        context.local_event("CS_ENTER", args.rid, resource="post-write", grant_token=grant["token"])
        write_json(args.ready, {"grant": grant})
        wait_for("controller to release test barrier", lambda: Path(args.gate).exists(), 90)
        if args.user_id:
            key = int.from_bytes(hashlib.sha256(str(args.post_id).encode("ascii")).digest(), "big") % 32
            owner = wire.ok("POST", "http://127.0.0.1:7004/lookup", {"key": key}, args.rid)["owner"]
            record = {"record_id": f"post:{args.post_id}", "routing_key": key}
            found = wire.ok("POST", owner["url"] + "/records/get", record, args.rid)
            require(found.get("found") is True, "Barrier writer could not retrieve the post")
            post = found["record"]
            require(args.user_id not in post["likes"], "Barrier user's like already exists")
            post["likes"].append(args.user_id)
            # No retry after an ambiguous storage outcome; the grant stays occupied.
            wire.ok("POST", owner["url"] + "/records/put",
                    dict(record, value=post, operation_id=args.rid + ":barrier-like",
                         coordination=grant), args.rid)
            context.local_event("WRITE_COMPLETE", args.rid, resource="post-write", grant_token=grant["token"])
        context.local_event("CS_EXIT", args.rid, resource="post-write", grant_token=grant["token"])
        reply = leader_lock_call(service, wire, "/locks/release", {"grant": grant}, args.rid)
        require(reply.get("status") == "released", "Release was not confirmed")
        write_json(args.result, {"success": True, "grant": grant, "exchanges": wire.exchanges})
    finally:
        service.close()  # Deliberately does NOT release an uncertain write in finally.


def like_worker(args):
    import client
    client.SERVICES["post"] = args.post_url
    client.TIMEOUT_SECONDS = 90  # May wait for election; mutations still never retry.
    wait_for("client start barrier", lambda: Path(args.gate).exists(), 90)
    result = client.send_request("post", "POST", f"/posts/{args.post_id}/likes",
                                 {"user_id": args.user_id}, request_id=args.rid)
    write_json(args.result, result)
    require("error" not in result and result.get("success") is True,
            f"Client mutation not confirmed: {result}")


def make_cluster(runs_dir, hosts):
    # Reuse the existing process lifecycle/isolated-data harness without editing it.
    from demo_p2p import Cluster

    class CoordinationCluster(Cluster):
        def spawn(self, label, script, args, env=None):
            environment = {"COORDINATION_ENABLED": "1", "COORD_ROOT": self.root,
                           "ZK_HOSTS": hosts, "NODE_NAME": label, "PUBLISH_POST_EVENTS": "1"}
            environment.update(env or {})
            return super().spawn(label, script, args, env=environment)

    cluster = CoordinationCluster("coordination", runs_dir)
    cluster.root = "/social-network/m4-runs/" + uuid.uuid4().hex
    return cluster


def run_demo(runs_dir=None, hosts="127.0.0.1:2181"):
    preflight()
    from message_protocol import MessageContext
    from kazoo.client import KazooClient
    from coordination_state import grant_matches
    cluster = make_cluster(runs_dir or REPO / "coordination_runs", hosts)
    previous_cwd = Path.cwd()
    previous_env = {key: os.environ.get(key) for key in ("ZK_HOSTS", "COORD_ROOT")}
    service = None
    cleanup = KazooClient(hosts=hosts)
    summary = {"status": "FAIL", "root": cluster.root, "checks": [], "limitations": [
        "Coordinator crash only; ZooKeeper and Chord remain available.",
        "Outstanding-grant test uses a real controlled storage writer, not a Post hook."]}
    try:
        cluster.require_free_ports(PORTS)
        cleanup.start(timeout=10)
        cleanup.ensure_path(cluster.root)  # This run owns only this UUID namespace.
        os.environ.update(ZK_HOSTS=hosts, COORD_ROOT=cluster.root)
        os.chdir(cluster.dir)
        context = MessageContext("demo-controller")
        wire = Wire(context)
        for index in (1, 2, 3):
            label, port = f"coord-{index}", 5100 + index
            cluster.spawn(label, "coordinator_server.py", ["--name", label, "--port", port])
            cluster.wait_http(label, f"http://127.0.0.1:{port}/coordination/status")
        service = discovery(context)
        leader = wait_for("ready elected leader", service.leader_info)
        leaders = [wire.ok("GET", f"http://127.0.0.1:{p}/coordination/status")["is_leader"]
                 for p in (5101, 5102, 5103)]
        require(all(type(value) is bool for value in leaders) and sum(leaders) == 1,
                f"Expected one leader, got is_leader flags {leaders}")
        summary["checks"].append("one elected leader")

        cluster.start_ring()
        for label, script in (("broker", "broker_server.py"), ("profile", "profile_server.py")):
            cluster.spawn(label, script, [])
            port = 5005 if label == "broker" else 5001
            cluster.wait_http(label, f"http://127.0.0.1:{port}/health")
        for label, port in (("post-a", 5002), ("post-b", 5006)):
            cluster.spawn(label, "post_server.py", [], env={"POST_PORT": str(port),
                          "CHORD_ENTRY_PEER": "http://127.0.0.1:7004"})
            cluster.wait_http(label, f"http://127.0.0.1:{port}/health")
        cluster.spawn("notification", "notification_server.py", ["--broker-url", "http://127.0.0.1:5005"])
        cluster.wait_http("notification", "http://127.0.0.1:5004/health")
        cluster.spawn("feed", "feed_server.py", ["--broker-url", "http://127.0.0.1:5005"])
        cluster.wait_http("feed", "http://127.0.0.1:5003/health")
        # HTTP readiness precedes subscription in the existing services.
        def subscribed():
            text = "\n".join(p.read_text(errors="replace") for p in (cluster.dir / "logs").glob("*.stderr"))
            return "SUBSCRIBED topic=post.created callback=" in text and "SUBSCRIBED topic=post.liked callback=" in text
        wait_for("Feed and Notification subscriptions", subscribed, 30)
        created = wire.ok("POST", "http://127.0.0.1:5002/posts",
                          {"author": 7, "caption": "Milestone 4", "content": "demo.jpg"})
        require(created.get("event_published") is True, "post.created was not delivered")
        delivery = created.get("event_delivery", {})
        require(delivery.get("all_delivered") is True and delivery.get("delivered", 0) >= 2,
                "Creation did not confirm delivery to both Feed and Notification")
        post_id = created["post"]["id"]
        notes = wire.ok("GET", "http://127.0.0.1:5004/notifications/2")
        require(any(n.get("post_id") == post_id and n.get("type") == "new_post"
                    for n in notes.get("notifications", [])), "Missing new-post notification")
        summary["checks"].append("post.created delivered to Feed and Notification")
        sections, origins = [], []
        expected_likes = set()

        def start_worker(label, mode, rid, *, port=5002, user=0, gate=None):
            gate = gate or cluster.dir / f"{label}.go"
            ready, result = cluster.dir / f"{label}.ready.json", cluster.dir / f"{label}.result.json"
            args = ["--worker", mode, "--rid", rid, "--gate", gate, "--ready", ready,
                    "--result", result, "--post-id", post_id, "--user-id", user,
                    "--post-url", f"http://127.0.0.1:{port}"]
            cluster.spawn(label, "demo_coordination.py", args)
            return gate, ready, result

        def finished(label, result):
            code = cluster.wait_exit(label, 95)
            require(code == 0, f"{label} failed ({code}): {cluster.tail(label)}")
            return json.loads(result.read_text(encoding="utf-8"))

        def lock_status():
            current = service.leader_info()
            if not current:
                return None
            try:
                return wire.ok("GET", current["url"] + "/coordination/status")["state"]
            except requests.RequestException:
                return None

        def queued(ids):
            state = lock_status()
            return state and set(ids) <= {item["request_id"] for item in state["queue"]}

        def contention_round(number, failover=False):
            hold_rid = f"round-{number}-holder"
            holder = f"holder-{number}"
            hold_gate, ready, hold_result = start_worker(holder, "holder", hold_rid,
                                                        user=90 if failover else 0)
            cluster.wait_until("holder barrier", ready.exists, 30, holder)
            original_grant = json.loads(ready.read_text(encoding="utf-8"))["grant"]
            common_gate = cluster.dir / f"round-{number}-clients.go"
            ids = [f"round-{number}-like-a", f"round-{number}-like-b"]
            jobs = []
            for offset, (rid, port) in enumerate(zip(ids, (5002, 5006))):
                label = f"client-{number}-{offset}"
                _, _, result = start_worker(label, "like", rid, port=port,
                                             user=20 + number * 2 + offset, gate=common_gate)
                jobs.append((label, result))
            common_gate.touch()
            wait_for("both Post instances queued", lambda: queued(ids), 25)
            state = lock_status()
            require(grant_matches(state, original_grant), "Queued requests replaced existing holder")
            if failover:
                old = service.leader_info()
                label = next((name for name in cluster.procs if old["node_id"].startswith(name + ":")), None)
                require(label and label.startswith("coord-"), "Cannot identify owned leader process")
                cluster.kill(label)
                def replacement():
                    info = service.leader_info()
                    return info if info and info["node_id"] != old["node_id"] else None
                new = wait_for("replacement coordinator", replacement, 60)
                require(new["generation"] > old["generation"], "Leader generation did not advance")
                state = lock_status()
                require(state and grant_matches(state, original_grant), "Replacement lost the active grant")
                require(set(ids) <= {item["request_id"] for item in state["queue"]},
                        "Replacement lost waiting requests")
                for name, _ in jobs:
                    require(cluster.procs[name].poll() is None, "Waiter finished before holder released")
                summary["checks"].append("leader replaced with holder and queue preserved")
            hold_gate.touch()
            held = finished(holder, hold_result)
            summary.setdefault("holder_exchanges", []).extend(held["exchanges"])
            for label, result in jobs:
                require(finished(label, result).get("success") is True, "Like failed")
            sections.extend([hold_rid] + ids)
            origins.append(ids)
            feed = wire.ok("GET", "http://127.0.0.1:5003/feed/2")
            post = next((p for p in feed["posts"] if p["id"] == post_id), None)
            expected = {20 + number * 2, 21 + number * 2} | ({90} if failover else set())
            expected_likes.update(expected)
            require(post and expected_likes == set(post["likes"]),
                    f"Lost or unexpected likes: expected {expected_likes}, got {post}")
            require(len(post["likes"]) == len(set(post["likes"])), "Duplicate likes stored")
            summary["checks"].append(f"round {number}: both Post instances' updates preserved")

        contention_round(1)
        contention_round(2, failover=True)
        contention_round(3)
        events = read_events(cluster.dir)
        check_critical_sections(events, sections)
        for pair in origins:
            check_concurrent_origins(events, pair)
        for rid in [rid for pair in origins for rid in pair]:
            require(any(e["request_id"] == rid and e["event"] == "LOCK_WAIT" for e in events),
                    f"Missing LOCK_WAIT evidence for {rid}")
        summary["checks"] += ["causally ordered, exclusive critical sections",
                              "concurrent request origins", "timestamped request/reply exchanges"]
        summary["exchanges"] = wire.exchanges
        summary["status"] = "PASS"
        return summary
    except Exception as error:
        summary["error"] = str(error)
        raise
    finally:
        cluster.stop_all()
        if service:
            service.close()
        # Never clear the ordinary /social-network/m4 namespace or user databases.
        try:
            if cleanup.connected:
                cleanup.delete(cluster.root, recursive=True)
        except Exception as error:
            summary["cleanup_warning"] = str(error)
        finally:
            cleanup.stop()
            cleanup.close()
            os.chdir(previous_cwd)
            for key, value in previous_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            write_json(cluster.dir / "summary.json", summary)
            print(f"{summary['status']}: evidence in {cluster.dir}", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check required modules; start nothing")
    parser.add_argument("--zk-hosts", default=os.getenv("ZK_HOSTS", "127.0.0.1:2181"))
    parser.add_argument("--runs-dir", type=Path, default=REPO / "coordination_runs")
    parser.add_argument("--worker", choices=("like", "holder"), help=argparse.SUPPRESS)
    for flag in ("rid", "gate", "ready", "result", "post-url"):
        parser.add_argument("--" + flag, help=argparse.SUPPRESS)
    parser.add_argument("--post-id", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--user-id", type=int, default=0, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.worker:
            (like_worker if args.worker == "like" else holder_worker)(args)
        elif args.check:
            preflight()
            print("Required modules found. ZooKeeper availability and real behavior still need the demo.")
        else:
            run_demo(args.runs_dir, args.zk_hosts)
        return 0
    except Exception as error:
        print(f"FAILED/BLOCKED: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
