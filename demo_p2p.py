#!/usr/bin/env python3
"""Repeatable Chord P2P demo and integration harness (CECS 327 Milestone 3).

Everything here is REAL: separate chord_peer.py processes (one SQLite file
each), plus the real Profile, Post and Feed services. Nothing is mocked.
Each run uses a fresh directory under p2p_runs/ (peer databases, logs), so it
never touches the repo's peer_data/ or logs/ folders. Only processes started
by this script are ever stopped.

    python3 demo_p2p.py                    # membership demo, then crash demo
    python3 demo_p2p.py membership         # four peers, lookup, join/leave 18
    python3 demo_p2p.py crash              # separate: crash of peer 20
    python3 demo_p2p.py --skip-post-feed   # Chord only (reported as UNRUN)

Exit code: 0 = no check failed, 1 = at least one check failed. Checks that
could not run are listed as UNRUN, never as passed.

Peers are started with the flags documented in chord_peer.py:
    --id ID --port PORT --db-dir DIR [--bootstrap URL]
"""

import argparse
import hashlib
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import time
import uuid
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))

from chord_client import ChordClient, ChordError  # noqa: E402
from chord_utils import finger_start, post_routing_key  # noqa: E402

HOST = "127.0.0.1"
PEER_PORTS = {4: 7004, 12: 7012, 20: 7020, 28: 7028, 18: 7018}
INITIAL_RING = [4, 12, 20, 28]
JOINER = 18
ENTRY = 4
FOCUS_KEY = 17
DEMO_KEYS = (2, 8, 14, 17, 19, 25)
DEMO_ID_START = 1000
PROFILE_PORT, POST_PORT, FEED_PORT, FEED_GROUP_PORT = 5001, 5002, 5003, 6003
PROFILE_URL = f"http://{HOST}:{PROFILE_PORT}"
POST_URL = f"http://{HOST}:{POST_PORT}"
FEED_URL = f"http://{HOST}:{FEED_PORT}"
FEED_USER = 2  # profile_server.py starter data: user 2 follows users 7 and 9
DEFAULT_RUNS_DIR = REPO / "p2p_runs"
READY_SECONDS = 30
RECOVERY_SECONDS = 60


class HarnessError(Exception):
    """A real process could not be started or reached (setup problem)."""


# ---------------------------------------------------------------- oracle

def oracle_owner(key, ring):
    """Textbook Chord: the first peer at or clockwise after the key."""
    ring = sorted(ring)
    for peer_id in ring:
        if peer_id >= key:
            return peer_id
    return ring[0]


def oracle_backup(key, ring):
    """One backup, held by the owner's successor."""
    ring = sorted(ring)
    return ring[(ring.index(oracle_owner(key, ring)) + 1) % len(ring)]


def needed_ports(services=False, joiner=True):
    ports = [PEER_PORTS[p] for p in INITIAL_RING]
    if joiner:
        ports.append(PEER_PORTS[JOINER])
    if services:
        ports += [PROFILE_PORT, POST_PORT, FEED_PORT, FEED_GROUP_PORT]
    return ports


def port_in_use(port):
    with socket.socket() as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((HOST, port)) == 0


# ------------------------------------------------------------------ logs

LogEntry = namedtuple("LogEntry", "at event fields raw")
LINE_RE = re.compile(r"^\S+Z \[peer-(\d+)\] ([A-Z_]+)(?: (.*))?$")
FIELD_RE = re.compile(r"(\w+)=(\[[^\]]*\]|\S+)")


def parse_log_line(line):
    match = LINE_RE.match(line.rstrip("\n"))
    if not match:
        return None
    fields = dict(FIELD_RE.findall(match.group(3) or ""))
    return LogEntry(int(match.group(1)), match.group(2), fields, line.rstrip("\n"))


# --------------------------------------------------------------- cluster

class Cluster:
    """The real processes of one scenario and their isolated data."""

    def __init__(self, name, runs_dir):
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.dir = Path(runs_dir).resolve() / f"{stamp}-{name}-{uuid.uuid4().hex[:4]}"
        (self.dir / "logs").mkdir(parents=True)
        self.procs = {}
        self.stderr_files = {}

    def peer_url(self, peer_id):
        return f"http://{HOST}:{PEER_PORTS[peer_id]}"

    def require_free_ports(self, ports):
        busy = [port for port in ports if port_in_use(port)]
        if busy:
            raise HarnessError(
                f"ports already in use: {busy}. Stop whatever is using them "
                "(another demo run or a manually started peer/service).")

    def spawn(self, label, script, args, env=None):
        previous = self.stderr_files.pop(label, None)
        if previous is not None:
            previous.close()
        stderr = open(self.dir / "logs" / f"{label}.stderr", "a")
        environment = dict(os.environ)
        environment.update(env or {})
        environment["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            [sys.executable, str(REPO / script)] + [str(a) for a in args],
            cwd=self.dir, env=environment, stdout=subprocess.DEVNULL,
            stderr=stderr, start_new_session=True)
        self.procs[label] = proc
        self.stderr_files[label] = stderr
        return proc

    def tail(self, label):
        lines = []
        for name in (f"{label}.stderr", f"{label}.log"):
            path = self.dir / "logs" / name
            if path.exists():
                lines += path.read_text(errors="replace").splitlines()[-4:]
        return " | ".join(lines)[-600:]

    def assert_alive(self, label):
        code = self.procs[label].poll()
        if code is not None:
            raise HarnessError(f"{label} exited with code {code}: {self.tail(label)}")

    def wait_until(self, what, predicate, seconds, label=None):
        end = time.monotonic() + seconds
        last = None
        while time.monotonic() < end:
            if label:
                self.assert_alive(label)
            try:
                value = predicate()
                if value:
                    return value
            except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
                last = exc
            time.sleep(0.2)
        raise HarnessError(f"timed out after {seconds}s waiting for {what}"
                           + (f" (last error: {last})" if last else ""))

    def wait_ready(self, peer_id, seconds=READY_SECONDS):
        def ready():
            body = requests.get(self.peer_url(peer_id) + "/health", timeout=2).json()
            return (body.get("success") is True and body.get("status") == "ready"
                    and body.get("peer_id") == peer_id)
        self.wait_until(f"peer {peer_id} to report ready", ready, seconds,
                        f"peer_{peer_id}")

    def start_peer(self, peer_id, bootstrap=None):
        args = ["--id", peer_id, "--port", PEER_PORTS[peer_id],
                "--db-dir", self.dir / "peer_data" / str(peer_id)]
        if bootstrap is not None:
            args += ["--bootstrap", self.peer_url(bootstrap)]
        self.spawn(f"peer_{peer_id}", "chord_peer.py", args)
        self.wait_ready(peer_id)

    def start_ring(self):
        self.start_peer(INITIAL_RING[0])
        for peer_id in INITIAL_RING[1:]:
            self.start_peer(peer_id, bootstrap=INITIAL_RING[0])

    def wait_http(self, label, url, seconds=READY_SECONDS):
        def up():
            return requests.get(url, timeout=2).status_code == 200
        self.wait_until(f"{label} to answer {url}", up, seconds, label)

    def start_services(self):
        self.spawn("profile", "profile_server.py", [])
        self.wait_http("profile", PROFILE_URL + "/health")
        self.spawn("post", "post_server.py", [], env={
            "POST_HOST": HOST, "POST_PORT": str(POST_PORT),
            "CHORD_ENTRY_PEER": self.peer_url(ENTRY),
            "PUBLISH_POST_EVENTS": "0"})  # Broker is outside this demo
        self.wait_http("post", POST_URL + "/health")
        self.spawn("feed", "feed_server.py", [
            "--port", FEED_PORT, "--group-port", FEED_GROUP_PORT,
            "--profile-url", PROFILE_URL, "--post-url", POST_URL])
        self.wait_http("feed", FEED_URL + "/health")

    def state(self, peer_id):
        return requests.get(self.peer_url(peer_id) + "/state", timeout=3).json()

    def db_rows(self, peer_id):
        """Live post rows of one peer's SQLite file, keyed by post ID.

        Reads the generic peer_records table (versions + tombstones) and, for
        older peers, the original peer_posts table.
        """
        path = self.dir / "peer_data" / str(peer_id) / f"peer_{peer_id}.db"
        if not path.exists():
            return {}
        conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
            if "peer_records" in tables:
                rows = conn.execute("SELECT * FROM peer_records").fetchall()
            else:
                rows = conn.execute("SELECT * FROM peer_posts").fetchall()
        finally:
            conn.close()
        found = {}
        for row in rows:
            if "record_id" in row.keys():
                if not row["record_id"].startswith("post:") or row["deleted"]:
                    continue
                value = json.loads(row["value_json"])
                found[int(row["record_id"][5:])] = {
                    "id": value["id"], "routing_key": row["routing_key"],
                    "author": value["author"], "timestamp": value["timestamp"],
                    "content": value["content"], "caption": value["caption"],
                    "likes": value["likes"], "comments": value["comments"],
                    "role": row["role"]}
            else:
                found[row["post_id"]] = {
                    "id": row["post_id"], "routing_key": row["routing_key"],
                    "author": row["author"], "timestamp": row["timestamp"],
                    "content": row["content"], "caption": row["caption"],
                    "likes": json.loads(row["likes_json"]),
                    "comments": json.loads(row["comments_json"]),
                    "role": row["role"]}
        return found

    def raw_records(self, peer_id):
        """Every live record id in one peer's file (posts, author index, counter)."""
        path = self.dir / "peer_data" / str(peer_id) / f"peer_{peer_id}.db"
        if not path.exists():
            return {}
        conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
            if "peer_records" not in tables:
                return {}
            rows = conn.execute("SELECT record_id, role FROM peer_records "
                                "WHERE deleted = 0").fetchall()
        finally:
            conn.close()
        return dict(rows)

    def entries(self, peer_id=None, request_id=None):
        ids = [peer_id] if peer_id is not None else sorted(PEER_PORTS)
        found = []
        for pid in ids:
            path = self.dir / "logs" / f"peer_{pid}.log"
            if not path.exists():
                continue
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                entry = parse_log_line(line)
                if entry and (request_id is None
                              or entry.fields.get("request_id") == request_id):
                    found.append(entry)
        return found

    def request_id_of(self, event, peer_id):
        for entry in self.entries(peer_id):
            if entry.event == event and entry.fields.get("request_id", "-") != "-":
                return entry.fields["request_id"]
        return None

    def kill(self, label):
        proc = self.procs[label]
        proc.kill()
        proc.wait()

    def wait_exit(self, label, seconds):
        try:
            return self.procs[label].wait(seconds)
        except subprocess.TimeoutExpired:
            return None

    def stop_all(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        end = time.monotonic() + 5
        for proc in self.procs.values():
            try:
                proc.wait(max(0.1, end - time.monotonic()))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for handle in self.stderr_files.values():
            handle.close()


# ---------------------------------------------------------------- report

class Report:
    def __init__(self):
        self.results = []
        self.expected = []

    def expect(self, names):
        for name in names:
            if name not in self.expected:
                self.expected.append(name)

    def _add(self, status, name, detail):
        self.results.append({"status": status, "name": name, "detail": detail})
        print(f"  [{status}] {name}" + (f"\n         {detail}" if detail else ""),
              flush=True)

    def ok(self, name, detail=""):
        self._add("PASS", name, detail)

    def fail(self, name, detail):
        self._add("FAIL", name, detail)

    def unrun(self, name, reason):
        self._add("UNRUN", name, reason)

    def check(self, name, problems, detail=""):
        if problems:
            shown = "; ".join(problems[:6])
            if len(problems) > 6:
                shown += f" (+{len(problems) - 6} more)"
            self.fail(name, shown)
        else:
            self.ok(name, detail)

    def finish(self):
        recorded = {r["name"] for r in self.results}
        for name in self.expected:
            if name not in recorded:
                self.unrun(name, "not reached (an earlier step failed or aborted)")

    def summary(self, runs_dir):
        counts = {s: sum(r["status"] == s for r in self.results)
                  for s in ("PASS", "FAIL", "UNRUN")}
        print("\n=== RESULT SUMMARY ===")
        for status in ("FAIL", "UNRUN", "PASS"):
            for r in self.results:
                if r["status"] == status:
                    print(f"  {status:5} {r['name']}")
        print(f"\npassed={counts['PASS']} failed={counts['FAIL']} unrun={counts['UNRUN']}")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        path = Path(runs_dir) / f"summary-{stamp}.json"
        Path(runs_dir).mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.results, indent=2), encoding="utf-8")
        print(f"summary written to {path}")
        return 1 if counts["FAIL"] else 0


def say(text=""):
    print(f"  {text}", flush=True)


def banner(title):
    print(f"\n=== {title} ===", flush=True)


# ------------------------------------------------------------- test data

def post_id_for_key(key, start=DEMO_ID_START, skip=()):
    """First post ID >= start whose REAL routing key (chord_utils) is `key`."""
    for post_id in range(start, start + 100000):
        if post_id not in skip and post_routing_key(post_id) == key:
            return post_id
    raise HarnessError(f"no post ID found for key {key}")


def make_demo_posts(keys=DEMO_KEYS, start=DEMO_ID_START):
    posts = {}
    for index, key in enumerate(keys):
        post_id = post_id_for_key(key, start, skip=posts)
        posts[post_id] = {
            "id": post_id, "author": 7 if index % 2 == 0 else 9,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "content": f"demo-photo-{post_id}.jpg",
            "caption": f"demo post {post_id} (routing key {key})",
            "likes": [], "comments": []}
    return posts


def new_request_id(tag):
    return f"demo-{tag}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------- verification
# Each function returns a list of problems; an empty list means it holds.

def ring_problems(cluster, ring):
    ring = sorted(ring)
    problems = []
    for index, peer_id in enumerate(ring):
        want_pred = ring[index - 1]
        want_succ = ring[(index + 1) % len(ring)]
        try:
            state = cluster.state(peer_id)
        except (requests.RequestException, ValueError) as exc:
            problems.append(f"peer {peer_id} /state unreachable: {exc}")
            continue
        if not state.get("ready"):
            problems.append(f"peer {peer_id} not ready (status={state.get('status')})")
        pred = (state.get("predecessor") or {}).get("id")
        succ = (state.get("successor") or {}).get("id")
        if pred != want_pred:
            problems.append(f"peer {peer_id} predecessor {pred}, expected {want_pred}")
        if succ != want_succ:
            problems.append(f"peer {peer_id} successor {succ}, expected {want_succ}")
        got = [f["node"]["id"] for f in state.get("fingers", [])]
        want = [oracle_owner(finger_start(peer_id, i), ring) for i in range(5)]
        if got != want:
            problems.append(f"peer {peer_id} fingers {got}, expected {want}")
    return problems


def placement_problems(cluster, posts, ring, also_empty=()):
    """Read every peer's SQLite file: right primary, right backup, nobody else."""
    problems = []
    tables = {pid: cluster.db_rows(pid) for pid in list(ring) + list(also_empty)}
    for post_id, post in posts.items():
        key = post_routing_key(post_id)
        owner, backup = oracle_owner(key, ring), oracle_backup(key, ring)
        for peer_id, table in tables.items():
            row = table.get(post_id)
            want = "primary" if peer_id == owner else "backup" if peer_id == backup else None
            label = f"post {post_id} (key {key}) on peer {peer_id}"
            if want is None:
                if row is not None:
                    problems.append(f"{label}: unexpected {row['role']} copy")
                continue
            if row is None:
                problems.append(f"{label}: missing, expected {want}")
                continue
            if row["role"] != want:
                problems.append(f"{label}: role {row['role']}, expected {want}")
            if row["routing_key"] != key:
                problems.append(f"{label}: routing_key {row['routing_key']}, expected {key}")
            for field in ("author", "timestamp", "content", "caption", "likes", "comments"):
                if row[field] != post[field]:
                    problems.append(f"{label}: {field} differs from what was stored")
    return problems


def raw_lookup(cluster, key, request_id, entry=ENTRY):
    response = requests.post(cluster.peer_url(entry) + "/lookup", json={"key": key},
                             headers={"X-Request-ID": request_id}, timeout=5)
    return response.status_code, response.json()


def lookup_problems(status, body, expect_owner, entry=ENTRY):
    if status != 200 or body.get("success") is not True:
        return [f"lookup failed: HTTP {status} {body}"]
    problems = []
    path = body.get("path", [])
    if body["owner"]["id"] != expect_owner:
        problems.append(f"owner {body['owner']['id']}, expected {expect_owner}")
    if not path or path[0] != entry:
        problems.append(f"path {path} does not start at entry peer {entry}")
    if body["owner"]["id"] == entry:
        problems.append("lookup started at the owner; pick a key owned elsewhere")
    if len(set(path)) != len(path):
        problems.append(f"path {path} repeats a peer")
    return problems


def lookup_trace_problems(cluster, request_id, body):
    """The peers' own logs must show this request travelling the returned path."""
    path, owner = body["path"], body["owner"]["id"]
    entries = cluster.entries(request_id=request_id)

    def has(at, event, **want):
        return any(e.at == at and e.event == event
                   and all(e.fields.get(k) == str(v) for k, v in want.items())
                   for e in entries)

    problems = []
    if not has(path[0], "RECV", path="/lookup"):
        problems.append(f"peer {path[0]} has no RECV /lookup for {request_id}")
    if len(path) > 1 and not any(
            e.at == path[0] and e.event == "LOOKUP"
            and e.fields.get("decision", "").startswith("forward") for e in entries):
        problems.append(f"peer {path[0]} logged no forwarding decision")
    for here, nxt in zip(path, path[1:]):
        if not has(here, "SEND", dest=nxt, path="/lookup"):
            problems.append(f"peer {here} has no SEND to {nxt}")
        if not has(nxt, "RECV", path="/lookup"):
            problems.append(f"peer {nxt} has no RECV for the forwarded lookup")
    final = [e for e in entries if e.at == path[-1] and e.event == "LOOKUP"]
    if not final or final[-1].fields.get("owner") != str(owner):
        problems.append(f"peer {path[-1]} logged no final decision naming owner {owner}")
    return problems


def store_posts(cluster, client, posts, ring, show_first=False):
    """Put each post through the overlay and verify the put and its log trail."""
    problems = []
    for index, (post_id, post) in enumerate(posts.items()):
        key = post_routing_key(post_id)
        rid = new_request_id(f"put-{post_id}")
        owner, backup = oracle_owner(key, ring), oracle_backup(key, ring)
        try:
            reply = client.put(f"post:{post_id}", key, post, rid)
        except ChordError as exc:
            problems.append(f"put post {post_id} failed: {exc}")
            continue
        if reply.get("stored") is not True or reply.get("role") != "primary":
            problems.append(f"put post {post_id} reply {reply}")
        if reply.get("backup") != backup:
            problems.append(f"post {post_id} backup on {reply.get('backup')}, expected {backup}")
        entries = cluster.entries(request_id=rid)
        if not any(e.at == owner and e.event == "RECORD_PUT"
                   and e.fields.get("backup_on") == str(backup) for e in entries):
            problems.append(f"peer {owner} logged no RECORD_PUT with backup_on={backup}")
        if not any(e.at == backup and e.event == "IMPORTED"
                   and e.fields.get("role") == "backup" for e in entries):
            problems.append(f"peer {backup} logged no backup IMPORTED")
        if show_first and index == 0:
            show_trace(cluster, f"store post {post_id} (key {key}) via entry peer {ENTRY}",
                       rid, {"RECV", "SEND", "LOOKUP", "RECORD_PUT", "IMPORTED"})
    return problems


def fetch_problems(client, posts):
    """Fetch every post through the overlay (lookup, then owner) and compare."""
    problems = []
    for post_id, post in posts.items():
        key = post_routing_key(post_id)
        try:
            record = client.get(f"post:{post_id}", key, new_request_id(f"get-{post_id}"))
        except ChordError as exc:
            problems.append(f"fetch post {post_id} (key {key}) failed: {exc}")
            continue
        if record != post:
            problems.append(f"fetch post {post_id} returned {record}, stored {post}")
    return problems


def transfer_problems(cluster, request_id, expected):
    """expected: (source, dest, 'from->to role', count) lines for one request."""
    if request_id is None:
        return ["could not find the request ID of the membership change in the logs"]
    transfers = [e for e in cluster.entries(request_id=request_id) if e.event == "TRANSFER"]
    problems = []
    for source, dest, roles, count in expected:
        match = [e for e in transfers if e.fields.get("source") == str(source)
                 and e.fields.get("dest") == str(dest) and e.fields.get("role") == roles]
        if not match:
            problems.append(f"no TRANSFER {source}->{dest} {roles}")
        elif not any(e.fields.get("count") == str(count) for e in match):
            problems.append(f"TRANSFER {source}->{dest} {roles} count "
                            f"{[e.fields.get('count') for e in match]}, expected {count}")
    return problems


def record_key(record_id):
    """Contract rule for author:<n> and meta:* records: SHA-256 of the ID, mod 32."""
    return int.from_bytes(hashlib.sha256(record_id.encode("utf-8")).digest(), "big") % 32


def extra_record_ids(created):
    """The non-post records Post keeps on the peers once it has created posts."""
    if not created:
        return []
    authors = sorted({post["author"] for post in created.values()})
    return ["meta:next_post_id"] + [f"author:{a}" for a in authors]


def record_placement_problems(cluster, record_ids, ring, also_empty=()):
    problems = []
    tables = {pid: cluster.raw_records(pid) for pid in list(ring) + list(also_empty)}
    for record_id in record_ids:
        key = record_key(record_id)
        owner, backup = oracle_owner(key, ring), oracle_backup(key, ring)
        for peer_id, table in tables.items():
            role = table.get(record_id)
            want = "primary" if peer_id == owner else "backup" if peer_id == backup else None
            if role != want:
                problems.append(f"{record_id} (key {key}) on peer {peer_id}: "
                                f"{role or 'absent'}, expected {want or 'absent'}")
    return problems


def count_owned(posts, ring, peer_id, extra_keys=()):
    keys = [post_routing_key(pid) for pid in posts] + list(extra_keys)
    return sum(1 for key in keys if oracle_owner(key, ring) == peer_id)


# ----------------------------------------------------------- Post / Feed

def post_log_hint(cluster, request_id):
    path = cluster.dir / "logs" / "post.log"
    if path.exists():
        for line in path.read_text(errors="replace").splitlines():
            if "CHORD_FAILURE" in line and request_id in line:
                return line.split("CHORD_FAILURE", 1)[1].strip()[:200]
    return "no CHORD_FAILURE line in post.log"


def create_posts_via_post(cluster):
    """Create posts through the real Post service. Returns (created, failures)."""
    created, failures = {}, []
    plan = [(7, "post-a.jpg", "first post by 7"), (9, "post-b.jpg", "post by 9"),
            (7, "post-c.jpg", "second post by 7")]
    for author, content, caption in plan:
        rid = new_request_id("create")
        try:
            reply = requests.post(POST_URL + "/posts", timeout=20,
                                  json={"author": author, "content": content,
                                        "caption": caption},
                                  headers={"X-Request-ID": rid})
            body = reply.json()
        except (requests.RequestException, ValueError) as exc:
            failures.append(f"POST /posts for author {author}: {exc}")
            break
        if reply.status_code != 201 or body.get("success") is not True or "post" not in body:
            failures.append(f"POST /posts for author {author} -> HTTP {reply.status_code} "
                            f"{reply.text.strip()[:100]}; post.log: "
                            f"{post_log_hint(cluster, rid)}")
            break
        created[body["id"]] = body["post"]
    return created, failures


def post_feed_problems(created):
    """Created posts must come back from Post and, unchanged, from Feed."""
    problems = []
    authors = {}
    for post in created.values():
        authors.setdefault(post["author"], {})[post["id"]] = post
    for author, expected in authors.items():
        try:
            reply = requests.post(POST_URL + "/posts/retrieve", json={"author": author},
                                  timeout=20)
            body = reply.json()
        except (requests.RequestException, ValueError) as exc:
            problems.append(f"/posts/retrieve author {author}: {exc}")
            continue
        if reply.status_code != 200 or body.get("success") is not True:
            problems.append(f"/posts/retrieve author {author} -> HTTP {reply.status_code}")
            continue
        got = {p["id"]: p for p in body["posts"]}
        for post_id, post in expected.items():
            if got.get(post_id) != post:
                problems.append(f"author {author}: post {post_id} missing or changed")
    try:
        reply = requests.get(f"{FEED_URL}/feed/{FEED_USER}", timeout=30)
        body = reply.json()
    except (requests.RequestException, ValueError) as exc:
        return problems + [f"feed for user {FEED_USER}: {exc}"]
    if reply.status_code != 200 or body.get("success") is not True:
        return problems + [f"feed for user {FEED_USER} -> HTTP {reply.status_code}"]
    posts = body["posts"]
    ids = [p["id"] for p in posts]
    if len(ids) != len(set(ids)):
        problems.append("feed contains duplicate post IDs")
    if [p["timestamp"] for p in posts] != sorted((p["timestamp"] for p in posts),
                                                 reverse=True):
        problems.append("feed is not newest first")
    by_id = {p["id"]: p for p in posts}
    for post_id, post in created.items():
        if by_id.get(post_id) != post:
            problems.append(f"feed is missing or changed post {post_id}")
    if body.get("following_source") != "profile":
        problems.append("feed did not take following from the real Profile service")
    return problems


# --------------------------------------------------------------- display

def show_trace(cluster, title, request_id, events):
    lines = sorted((e for e in cluster.entries(request_id=request_id)
                    if e.event in events), key=lambda e: e.raw.split(" ", 1)[0])
    header = f"peer messages for request {request_id}: {title}"
    say(header)
    say("(log timestamps have 1-second resolution; order inside a second is approximate)")
    with open(cluster.dir / "logs" / "demo_trace.txt", "a", encoding="utf-8") as out:
        out.write(f"## {header}\n")
        for entry in lines:
            say("  " + entry.raw)
            out.write(entry.raw + "\n")
        out.write("\n")


def describe_placement(cluster, posts, peer_ids):
    tables = {pid: cluster.db_rows(pid) for pid in peer_ids}
    for post_id in sorted(posts, key=post_routing_key):
        where = ", ".join(f"peer {pid}={tables[pid][post_id]['role']}"
                          for pid in peer_ids if post_id in tables[pid])
        say(f"post {post_id} key {post_routing_key(post_id):>2}: {where}")


# ----------------------------------------------------- membership demo

M = {
    "start": "membership 01: four peers start ready; ring links and finger tables correct",
    "services": "membership 02: Profile, Post and Feed start against the peers",
    "pf_create": "membership 03: Post creates posts over the real peers",
    "pf_base": "membership 04: Post retrieve and Feed return the created posts",
    "store": "membership 05: posts stored through the overlay (primary + backup on successor)",
    "lookup": "membership 06: lookup for key 17 entered at peer 4 routes to owner peer 20",
    "lookup_log": "membership 07: peer logs show that lookup (request ID, hops, decisions)",
    "fetch": "membership 08: resource discovered by lookup and fetched with stored content",
    "join": "membership 09: peer 18 joins; ring, neighbours and finger tables updated",
    "join_data": "membership 10: join moved the affected data; key 17 is now owned by peer 18",
    "join_log": "membership 11: join TRANSFER log lines match the data that moved",
    "join_fetch": "membership 12: every post still retrievable through the overlay after join",
    "join_pf": "membership 13: Post retrieve and Feed still correct after join",
    "leave": "membership 14: peer 18 graceful leave accepted and its process exits",
    "leave_ring": "membership 15: neighbours and fingers updated; key 17 owner back to peer 20",
    "leave_data": "membership 16: leave handed data off; placement and content correct",
    "leave_log": "membership 17: leave TRANSFER log lines match the data handed off",
    "leave_fetch": "membership 18: every post still retrievable through the overlay after leave",
    "leave_pf": "membership 19: Post retrieve and Feed still correct after leave",
}
PF_KEYS = ("services", "pf_create", "pf_base", "join_pf", "leave_pf")


def run_membership(rep, runs_dir, with_post_feed):
    banner("MEMBERSHIP DEMO: four peers, lookup, store/fetch, peer 18 join, peer 18 leave")
    rep.expect(list(M.values()))
    if not with_post_feed:
        for key in PF_KEYS:
            rep.unrun(M[key], "skipped by --skip-post-feed")
    cluster = Cluster("membership", runs_dir)
    say(f"run directory (databases and logs): {cluster.dir}")
    try:
        cluster.require_free_ports(needed_ports(services=with_post_feed))

        banner("1. Start four separate peer processes, each with its own database")
        cluster.start_ring()
        rep.check(M["start"], ring_problems(cluster, INITIAL_RING),
                  "peers 4, 12, 20, 28 ready on ports 7004/7012/7020/7028")

        created, pf_block = {}, None
        if with_post_feed:
            banner("2. Start Profile, Post and Feed; create posts through Post")
            try:
                cluster.start_services()
                rep.ok(M["services"], "Profile 5001, Post 5002, Feed 5003 answering /health")
                created, failures = create_posts_via_post(cluster)
                if failures:
                    pf_block = failures[0]
                    rep.fail(M["pf_create"], pf_block)
                else:
                    rep.ok(M["pf_create"], f"created post IDs {sorted(created)}")
                    rep.check(M["pf_base"], post_feed_problems(created))
            except HarnessError as exc:
                pf_block = f"services did not start: {exc}"
                rep.fail(M["services"], pf_block)
            if pf_block:
                for key in ("pf_base", "join_pf", "leave_pf"):
                    if not any(r["name"] == M[key] for r in rep.results):
                        rep.unrun(M[key], "blocked by the Post/service failure above")

        demo_posts = make_demo_posts()
        all_posts = {**demo_posts, **created}
        extra_ids = extra_record_ids(created)
        extra_keys = [record_key(r) for r in extra_ids]
        client = ChordClient(cluster.peer_url(ENTRY), timeout=5.0)
        ring = list(INITIAL_RING)

        banner("3. Store resources through the overlay (post IDs chosen with the real hash)")
        for pid, post in demo_posts.items():
            say(f"post ID {pid} hashes to key {post_routing_key(pid)} -> owner "
                f"{oracle_owner(post_routing_key(pid), ring)}, backup "
                f"{oracle_backup(post_routing_key(pid), ring)}")
        problems = store_posts(cluster, client, demo_posts, ring, show_first=True)
        problems += placement_problems(cluster, all_posts, ring)
        problems += record_placement_problems(cluster, extra_ids, ring)
        rep.check(M["store"], problems)
        describe_placement(cluster, all_posts, ring)

        banner(f"4. Routed lookup for key {FOCUS_KEY}, starting at peer {ENTRY} (not the owner)")
        focus_id = post_id_for_key(FOCUS_KEY)
        rid = new_request_id("lookup")
        status, body = raw_lookup(cluster, FOCUS_KEY, rid)
        problems = lookup_problems(status, body, oracle_owner(FOCUS_KEY, ring))
        rep.check(M["lookup"], problems,
                  f"owner {body.get('owner')}, hops={body.get('hops')}, path={body.get('path')}")
        if problems:
            rep.unrun(M["lookup_log"], "blocked by the lookup failure above")
        else:
            show_trace(cluster, f"lookup key {FOCUS_KEY}", rid,
                       {"RECV", "SEND", "LOOKUP"})
            rep.check(M["lookup_log"], lookup_trace_problems(cluster, rid, body))

        banner(f"5. Resource discovery: fetch post {focus_id} from the owner found by lookup")
        rid = new_request_id("fetch")
        try:
            record = client.get(f"post:{focus_id}", FOCUS_KEY, rid)
        except ChordError as exc:
            record = None
            problems = [f"fetch failed: {exc}"]
        else:
            problems = [] if record == demo_posts[focus_id] else [
                f"fetched {record}, stored {demo_posts[focus_id]}"]
            owner = oracle_owner(FOCUS_KEY, ring)
            if not any(e.at == owner and e.event == "RECORD_GET"
                       and e.fields.get("found") == "True" for e in cluster.entries(request_id=rid)):
                problems.append(f"peer {owner} logged no successful RECORD_GET")
        rep.check(M["fetch"], problems)
        say(f"fetched content: {json.dumps(record)}")
        show_trace(cluster, f"fetch post {focus_id}", rid,
                   {"RECV", "SEND", "LOOKUP", "RECORD_GET"})

        banner(f"6. Peer {JOINER} joins (bootstrap peer {ENTRY})")
        before, after = list(ring), sorted(ring + [JOINER])
        cluster.start_peer(JOINER, bootstrap=ENTRY)
        join_rid = cluster.request_id_of("JOIN_COMPLETE", JOINER)
        problems = ring_problems(cluster, after)
        rep.check(M["join"], problems, f"ring is now {after}")
        status, body = raw_lookup(cluster, FOCUS_KEY, new_request_id("lookup-after-join"))
        problems = lookup_problems(status, body, JOINER)
        problems += placement_problems(cluster, all_posts, after)
        problems += record_placement_problems(cluster, extra_ids, after)
        rep.check(M["join_data"], problems,
                  f"key {FOCUS_KEY} now resolves to {body.get('owner')} via path {body.get('path')}")
        describe_placement(cluster, all_posts, after)
        pred = after[after.index(JOINER) - 1]
        old_owner = oracle_owner(JOINER, before)
        moved = sum(1 for key in [post_routing_key(pid) for pid in all_posts] + extra_keys
                    if oracle_owner(key, before) == old_owner
                    and oracle_owner(key, after) == JOINER)
        expected = [(old_owner, JOINER, "primary->primary", moved),
                    (old_owner, JOINER, "backup->backup",
                     count_owned(all_posts, before, pred, extra_keys))]
        if join_rid:
            show_trace(cluster, f"peer {JOINER} join", join_rid,
                       {"TRANSFER", "ROLE_SET", "DROPPED", "SUCCESSOR_SET",
                        "PREDECESSOR_SET", "JOIN_COMPLETE"})
        rep.check(M["join_log"], transfer_problems(cluster, join_rid, expected))
        rep.check(M["join_fetch"], fetch_problems(client, all_posts))
        if with_post_feed and not pf_block:
            rep.check(M["join_pf"], post_feed_problems(created))

        banner(f"7. Peer {JOINER} gracefully leaves (POST /leave)")
        leave_rid = new_request_id("leave")
        problems = []
        try:
            reply = requests.post(cluster.peer_url(JOINER) + "/leave", timeout=60,
                                  headers={"X-Request-ID": leave_rid})
            body = reply.json()
            if reply.status_code != 200 or body.get("status") != "left":
                problems.append(f"/leave -> HTTP {reply.status_code} {body}")
        except (requests.RequestException, ValueError) as exc:
            problems.append(f"/leave request failed: {exc}")
        code = cluster.wait_exit(f"peer_{JOINER}", 15)
        if code is None:
            problems.append(f"peer {JOINER} process did not exit after leave")
        elif code != 0:
            problems.append(f"peer {JOINER} exited with code {code}")
        if port_in_use(PEER_PORTS[JOINER]):
            problems.append(f"port {PEER_PORTS[JOINER]} is still open after leave")
        rep.check(M["leave"], problems, f"exit code {code}")
        show_trace(cluster, f"peer {JOINER} leave", leave_rid,
                   {"TRANSFER", "ROLE_SET", "DROPPED", "SUCCESSOR_SET",
                    "PREDECESSOR_SET", "LEAVE_COMPLETE"})
        status, body = raw_lookup(cluster, FOCUS_KEY, new_request_id("lookup-after-leave"))
        problems = ring_problems(cluster, before)
        problems += lookup_problems(status, body, oracle_owner(FOCUS_KEY, before))
        rep.check(M["leave_ring"], problems, f"ring is back to {before}")
        problems = placement_problems(cluster, all_posts, before, also_empty=[JOINER])
        problems += record_placement_problems(cluster, extra_ids, before,
                                              also_empty=[JOINER])
        rep.check(M["leave_data"], problems)
        describe_placement(cluster, all_posts, before)
        owned = count_owned(all_posts, after, JOINER, extra_keys)
        expected = [(JOINER, oracle_owner(JOINER + 1, after), "primary->primary", owned),
                    (JOINER, oracle_backup(JOINER + 1, after), "primary->backup", owned),
                    (JOINER, oracle_owner(JOINER + 1, after), "backup->backup",
                     count_owned(all_posts, after, pred, extra_keys))]
        rep.check(M["leave_log"], transfer_problems(cluster, leave_rid, expected))
        rep.check(M["leave_fetch"], fetch_problems(client, all_posts))
        if with_post_feed and not pf_block:
            rep.check(M["leave_pf"], post_feed_problems(created))
    except HarnessError as exc:
        rep.fail("membership scenario aborted", str(exc))
    finally:
        cluster.stop_all()
    say(f"logs saved in {cluster.dir / 'logs'}")


# ---------------------------------------------------------- crash demo

C = {
    "start": "crash 01: four peers start ready; ring links and finger tables correct",
    "backup": "crash 02: posts stored; every primary has its backup on the successor",
    "kill": "crash 03: peer 20 killed without leaving (SIGKILL); it held data",
    "recover": "crash 04: successor 28 detects the failure and completes recovery",
    "ring": "crash 05: ring and finger tables updated without peer 20",
    "data": "crash 06: backups promoted and re-backed up; placement and content correct",
    "fetch": "crash 07: key 17 now routed to peer 28 and every post retrievable",
    "log": "crash 08: recovery log lines show detection, promotion and re-backup transfers",
    "write": "crash 09: a new write after recovery gets a primary and a restored backup",
}
VICTIM, TAKEOVER = 20, 28


def run_crash(rep, runs_dir):
    banner("CRASH DEMO (separate from graceful leave): peer 20 dies unexpectedly")
    rep.expect(list(C.values()))
    cluster = Cluster("crash", runs_dir)
    say(f"run directory (databases and logs): {cluster.dir}")
    try:
        cluster.require_free_ports(needed_ports(joiner=False))
        cluster.start_ring()
        rep.check(C["start"], ring_problems(cluster, INITIAL_RING))
        posts = make_demo_posts()
        client = ChordClient(cluster.peer_url(ENTRY), timeout=5.0)
        ring = list(INITIAL_RING)
        problems = store_posts(cluster, client, posts, ring)
        problems += placement_problems(cluster, posts, ring)
        rep.check(C["backup"], problems)
        describe_placement(cluster, posts, ring)

        victim_primary = [pid for pid, row in cluster.db_rows(VICTIM).items()
                          if row["role"] == "primary"]
        backups_on_takeover = [pid for pid, row in cluster.db_rows(TAKEOVER).items()
                               if row["role"] == "backup"]
        banner(f"Kill peer {VICTIM} with SIGKILL (no /leave)")
        cluster.kill(f"peer_{VICTIM}")
        killed_at = time.monotonic()
        problems = []
        if cluster.procs[f"peer_{VICTIM}"].poll() is None:
            problems.append("peer 20 is still running")
        if not victim_primary:
            problems.append("peer 20 held no primary posts, so the crash proves nothing")
        missing = set(victim_primary) - set(backups_on_takeover)
        if missing:
            problems.append(f"posts {sorted(missing)} had no backup on peer {TAKEOVER}")
        rep.check(C["kill"], problems,
                  f"peer 20 held primaries {sorted(victim_primary)}; peer {TAKEOVER} held "
                  f"their backups {sorted(set(victim_primary) & set(backups_on_takeover))}")

        say(f"waiting (deadline {RECOVERY_SECONDS}s) for peer {TAKEOVER} to take over ...")
        survivors = [4, 12, 28]
        cluster.wait_until(
            f"peer {TAKEOVER} to log RECOVERY_COMPLETE",
            lambda: any(e.event == "RECOVERY_COMPLETE" for e in cluster.entries(TAKEOVER)),
            RECOVERY_SECONDS, f"peer_{TAKEOVER}")
        elapsed = time.monotonic() - killed_at
        recovery_rid = cluster.request_id_of("RECOVERY_COMPLETE", TAKEOVER)
        state = cluster.state(TAKEOVER)
        problems = []
        if (state.get("predecessor") or {}).get("id") != 12 or not state.get("ready"):
            problems.append(f"peer {TAKEOVER} state after recovery: {state.get('predecessor')}, "
                            f"ready={state.get('ready')}")
        rep.check(C["recover"], problems, f"recovery completed {elapsed:.1f}s after the kill")
        show_trace(cluster, "crash recovery", recovery_rid,
                   {"TRANSFER", "SUCCESSOR_SET", "PREDECESSOR_SET", "RECOVERY_START",
                    "RECOVERY_COMPLETE"})

        rep.check(C["ring"], ring_problems(cluster, survivors),
                  f"ring is now {survivors}")
        rep.check(C["data"], placement_problems(cluster, posts, survivors))
        describe_placement(cluster, posts, survivors)
        status, body = raw_lookup(cluster, FOCUS_KEY, new_request_id("lookup-after-crash"))
        problems = lookup_problems(status, body, oracle_owner(FOCUS_KEY, survivors))
        problems += fetch_problems(client, posts)
        rep.check(C["fetch"], problems,
                  f"key {FOCUS_KEY} -> {body.get('owner')} via {body.get('path')}")

        problems = []
        unreachable = [e for e in cluster.entries(TAKEOVER)
                       if e.event == "PREDECESSOR_UNREACHABLE" and e.fields.get("peer") == str(VICTIM)]
        if len(unreachable) < 3:
            problems.append(f"expected >=3 PREDECESSOR_UNREACHABLE for peer {VICTIM}, "
                            f"saw {len(unreachable)}")
        if not any(e.event == "RECOVERY_START" and e.fields.get("dead") == str(VICTIM)
                   for e in cluster.entries(TAKEOVER, recovery_rid)):
            problems.append("no RECOVERY_START naming the dead peer")
        if not any(e.event == "RECOVERY_COMPLETE" and e.fields.get("new_predecessor") == "12"
                   for e in cluster.entries(TAKEOVER, recovery_rid)):
            problems.append("no RECOVERY_COMPLETE with new_predecessor=12")
        problems += transfer_problems(cluster, recovery_rid, [
            (TAKEOVER, 4, "primary->backup", count_owned(posts, survivors, TAKEOVER)),
            (12, TAKEOVER, "primary->backup", count_owned(posts, survivors, 12))])
        rep.check(C["log"], problems)

        extra = make_demo_posts(keys=(9,), start=DEMO_ID_START + 500)
        problems = store_posts(cluster, client, extra, survivors)
        problems += placement_problems(cluster, {**posts, **extra}, survivors)
        rep.check(C["write"], problems)
    except HarnessError as exc:
        rep.fail("crash scenario aborted", str(exc))
    finally:
        cluster.stop_all()
    say(f"logs saved in {cluster.dir / 'logs'}")


# ------------------------------------------------------------------ main

def main(argv=None):
    parser = argparse.ArgumentParser(description="Repeatable Chord P2P demo")
    parser.add_argument("scenario", nargs="?", default="all",
                        choices=["all", "membership", "crash"])
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR),
                        help="where isolated run folders are created")
    parser.add_argument("--skip-post-feed", action="store_true",
                        help="do not start Profile/Post/Feed (those checks are UNRUN)")
    args = parser.parse_args(argv)

    rep = Report()
    try:
        if args.scenario in ("all", "membership"):
            run_membership(rep, args.runs_dir, not args.skip_post_feed)
        if args.scenario in ("all", "crash"):
            run_crash(rep, args.runs_dir)
    except KeyboardInterrupt:
        print("\ninterrupted; demo processes were stopped", flush=True)
        rep.fail("demo interrupted", "stopped by the user")
    rep.finish()
    return rep.summary(args.runs_dir)


if __name__ == "__main__":
    sys.exit(main())
