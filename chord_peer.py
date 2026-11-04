"""

In seperate terminals, run:
    python chord_peer.py --id 4  --port 7004                                         # first peer: new ring
    python chord_peer.py --id 12 --port 7012 --bootstrap http://127.0.0.1:7004
    python chord_peer.py --id 20 --port 7020 --bootstrap http://127.0.0.1:7004
    python chord_peer.py --id 28 --port 7028 --bootstrap http://127.0.0.1:7004
    python chord_peer.py --id 18 --port 7018 --bootstrap http://127.0.0.1:7004       # join demo
    curl -X POST http://127.0.0.1:7018/leave                                         # leave demo


PUBLIC API (chord_contract.md)
    GET  /health              {"success", "peer_id", "status"}
    GET  /state               neighbours, fingers, readiness, record counts
    POST /lookup              {"key": 17} -> {"owner": {"id", "url"}, "hops", "path"}
    POST /join                {"bootstrap_url": "..."}  (also done by --bootstrap)
    POST /leave               graceful leave, then the process exits
    POST /records/get|put|delete   record operations on the owner

INTERNAL API (peer-to-peer only, used by join/leave/recovery)
    /internal/pause, /internal/set_successor, /internal/set_predecessor,
    /internal/export, /internal/import, /internal/set_role, /internal/drop,
    /internal/refresh, /internal/rebackup

RECORDS
    "post:<n>", "author:<n>" and "meta:next_post_id" are all stored. Every
    record carries a version; a delete leaves a tombstone that is copied to
    the backup, so a delayed old copy can never overwrite or undo it.

LIMITS (see the report/README)
    * One membership change at a time (enforced with a ring-wide pause).
    * Tombstones are never purged (fine for a class demo).
    * "meta:next_post_id" is read then written by Post; with ONE Post process
      that is safe (the contract assumes one).
"""

import argparse
import hashlib
import logging
import sys
import threading
import time
import uuid
from pathlib import Path

import requests
from flask import Flask, g, jsonify, request
from werkzeug.serving import make_server

from chord_utils import (RING_SIZE, closest_preceding_finger, finger_start,
                         in_open_closed, post_routing_key)
from peer_storage import PeerStorage

FINGER_COUNT = 5            # 2^5 = 32 ring positions
MAX_HOPS = 8                # a lookup that needs more hops than this is broken
HTTP_TIMEOUT = 3.0          # seconds for one peer-to-peer request
HEALTH_INTERVAL = 2.0       # how often we ping our predecessor
FAILURES_BEFORE_RECOVERY = 3


class PeerUnreachable(Exception):
    """Another peer did not answer (connection refused or timed out)."""


class PeerError(Exception):
    """Another peer answered, but with an error."""


# Small helpers

def make_logger(peer_id, log_dir="logs"):
    """UTC log lines to the terminal and logs/peer_<id>.log, team style."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"peer-{peer_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)sZ [%(name)s] %(message)s",
                                  "%Y-%m-%dT%H:%M:%S")
    formatter.converter = time.gmtime
    for handler in (logging.StreamHandler(sys.stdout),
                    logging.FileHandler(Path(log_dir) / f"peer_{peer_id}.log")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def ref(peer_id, url):
    """The contract's peer address format: {"id": 12, "url": "http://..."}."""
    return {"id": peer_id, "url": url.rstrip("/")}


def valid_key(value):
    return type(value) is int and 0 <= value < RING_SIZE


def routing_key_for(record_id):
    """The ring key of a record ID, or None if the ID is not one we store.

    post:<n>            SHA-256 of the decimal post ID (chord_utils)
    author:<n>          SHA-256 of the UTF-8 record ID, modulo 32 (contract)
    meta:next_post_id   same rule as author records
    """
    if not isinstance(record_id, str):
        return None
    kind, _, rest = record_id.partition(":")
    if kind == "post" and rest.isascii() and rest.isdigit() and int(rest) > 0:
        return post_routing_key(int(rest))
    if (kind == "author" and rest.isascii() and rest.isdigit() and int(rest) > 0) \
            or record_id == "meta:next_post_id":
        digest = hashlib.sha256(record_id.encode("utf-8")).digest()
        return int.from_bytes(digest, "big") % RING_SIZE
    return None


# The peer

class ChordPeer:
    def __init__(self, peer_id, url, db_dir):
        self.me = ref(peer_id, url)
        self.id = peer_id
        self.storage = PeerStorage(peer_id, db_dir)
        self.log = make_logger(peer_id)

        # All routing state is protected by this lock.  We NEVER hold it while
        # making an HTTP call, so two peers calling each other can't deadlock.
        self.lock = threading.Lock()
        self.predecessor = None
        self.successors = [self.me]          # [successor, successor's successor]
        self.fingers = []                    # [{"start": s, "node": ref}, ...]
        self.status = "starting"             # starting/transferring/ready/recovering
        self.pause_id = None                 # set while a membership change runs
        self.pred_failures = 0
        self.server = None                   # set in main() so /leave can stop us

    #logging
    def info(self, event, **fields):
        """One log line: EVENT key=value ... (always includes the request ID)."""
        fields.setdefault("request_id", getattr(g, "request_id", "-")
                          if _in_request() else "-")
        text = " ".join(f"{k}={v}" for k, v in fields.items())
        self.log.info("%s %s", event, text)

    #  HTTP out
    def call(self, peer, path, body=None, request_id=None, method="POST",
             timeout=HTTP_TIMEOUT):
        """Send one JSON request to another peer and return its JSON reply.

        Raises PeerUnreachable if it doesn't answer, PeerError on an error reply.
        """
        request_id = request_id or str(uuid.uuid4())
        url = peer["url"] + path
        self.info("SEND", dest=peer["id"], path=path, request_id=request_id)
        try:
            response = requests.request(method, url, json=body, timeout=timeout,
                                        headers={"X-Request-ID": request_id})
            reply = response.json()
        except requests.RequestException as exc:
            self.info("SEND_FAILED", dest=peer["id"], path=path,
                      request_id=request_id, error=type(exc).__name__)
            raise PeerUnreachable(f"peer {peer['id']} unreachable") from exc
        except ValueError as exc:
            raise PeerError(f"peer {peer['id']} sent invalid JSON") from exc
        if not response.ok or reply.get("success") is not True:
            raise PeerError(f"peer {peer['id']} {path} -> HTTP "
                            f"{response.status_code}: {reply.get('error')}")
        return reply

    def state_of(self, peer, request_id=None):
        return self.call(peer, "/state", method="GET", request_id=request_id)

    # snapshot
    def snapshot(self):
        """Copy the routing state so we can use it without holding the lock."""
        with self.lock:
            return (self.predecessor, list(self.successors), list(self.fingers),
                    self.status, self.pause_id)

    def writes_allowed(self):
        _, successors, _, status, pause_id = self.snapshot()
        if status != "ready" or pause_id is not None:
            return False, "writes paused: membership change or recovery in progress"
        if successors[0]["id"] == self.id:
            return False, "no distinct successor to hold the backup"
        return True, None

    
    # Lookup: the Chord routing algorithm

    def lookup(self, key, hops=0, path=None, request_id=None):
        """Find the owner of `key` by routing through real peer requests.

        1. If key is in (predecessor, me]          -> I own it.
        2. If key is in (me, successor]            -> my successor owns it.
        3. Otherwise forward to the closest finger that comes BEFORE the key
           (a shortcut).  If that peer is down, fall back to my successor.
        Returns (owner_ref, hops, path).
        """
        path = (path or []) + [self.id]
        pred, successors, fingers, _, _ = self.snapshot()
        succ = successors[0]

        if succ["id"] == self.id:                       # one-peer ring
            self.info("LOOKUP", key=key, decision="single-peer-owns", owner=self.id)
            return self.me, hops, path
        if pred and in_open_closed(key, pred["id"], self.id):
            self.info("LOOKUP", key=key, decision="self-owns", owner=self.id)
            return self.me, hops, path
        if in_open_closed(key, self.id, succ["id"]):
            self.info("LOOKUP", key=key, decision="successor-owns", owner=succ["id"])
            return succ, hops, path

        if hops >= MAX_HOPS:
            raise PeerError(f"lookup for key {key} exceeded {MAX_HOPS} hops: {path}")

        # Candidate next hops, best first: finger shortcut, then successors.
        shortcut = closest_preceding_finger(self.id, key, fingers + successors)
        candidates = []
        for peer in [shortcut] + successors:
            if peer and peer["id"] != self.id and peer["id"] not in path \
                    and peer["id"] not in [c["id"] for c in candidates]:
                candidates.append(peer)

        for peer in candidates:
            decision = "finger" if shortcut and peer["id"] == shortcut["id"] \
                and peer["id"] != succ["id"] else "successor"
            self.info("LOOKUP", key=key, decision=f"forward-{decision}",
                      next=peer["id"], hops=hops + 1)
            try:
                reply = self.call(peer, "/lookup",
                                  {"key": key, "hops": hops + 1, "path": path},
                                  request_id)
                return reply["owner"], reply["hops"], reply["path"]
            except PeerUnreachable:
                continue                                 # try the next candidate
        raise PeerError(f"no reachable next hop for key {key} from peer {self.id}")

    
    # Ring-wide helpers
  
    def walk_ring(self, start, request_id):
        """Visit every peer by following successor pointers. Returns refs."""
        peers, current = [], start
        for _ in range(RING_SIZE + 1):
            if current["id"] in [p["id"] for p in peers]:
                return peers
            peers.append(current)
            current = self.state_of(current, request_id)["successor"]
        raise PeerError("ring walk did not return to the start")

    def pause_ring(self, change_id, paused, request_id):
        """Pause (or resume) writes on every peer for one membership change.

        A peer that is already paused by a DIFFERENT change answers 409, which
        is how we enforce 'one membership change at a time'.
        """
        for peer in self.walk_ring(self.successors[0], request_id):
            self.call(peer, "/internal/pause",
                      {"change_id": change_id, "paused": paused}, request_id)

    def refresh_ring(self, request_id):
        """After any membership change, every peer rebuilds its fingers."""
        for peer in self.walk_ring(self.me, request_id):
            self.call(peer, "/internal/refresh", {}, request_id)

    def refresh(self, request_id):
        """Rebuild my successor list and finger table using routed lookups."""
        _, successors, _, _, _ = self.snapshot()
        succ = successors[0]
        second = succ
        if succ["id"] != self.id:
            try:
                second = self.state_of(succ, request_id)["successor"]
            except (PeerUnreachable, PeerError):
                second = succ
        with self.lock:
            self.successors = [succ, second]
        fingers = []
        for i in range(FINGER_COUNT):
            start = finger_start(self.id, i)
            owner, _, _ = self.lookup(start, request_id=request_id)
            fingers.append({"start": start, "node": owner})
        with self.lock:
            self.fingers = fingers
        self.info("FINGERS_REFRESHED", fingers=[f["node"]["id"] for f in fingers],
                  successors=[s["id"] for s in self.successors],
                  request_id=request_id)

    #transfers
    def move(self, source, dest, start, end, from_role, to_role, request_id):
        """Copy records with keys in (start, end] from source to dest.

        Returns the number copied.  Raises if dest doesn't confirm every one,
        so callers never delete the source copy after a failed handoff.
        """
        records = self.call(source, "/internal/export",
                            {"start": start, "end": end, "role": from_role},
                            request_id)["records"]
        if records:
            reply = self.call(dest, "/internal/import",
                              {"records": records, "role": to_role}, request_id)
            if reply["imported"] != len(records):
                raise PeerError(f"peer {dest['id']} confirmed {reply['imported']}"
                                f" of {len(records)} records")
        self.info("TRANSFER", source=source["id"], dest=dest["id"],
                  interval=f"({start},{end}]", role=f"{from_role}->{to_role}",
                  count=len(records), request_id=request_id)
        return len(records)

   
    # Join
  
    def join(self, bootstrap_url, request_id):
        """Join the ring through any existing peer.

        Example: peer 18 joins the ring [4, 12, 20, 28].
          O  = owner of key 18 = 20       (my new successor)
          P  = O's predecessor  = 12      (my new predecessor)
          S  = O's successor    = 28      (held backups of (12,18])
        Before:  (12,18] primary on 20, backup on 28
        After:   (12,18] primary on 18, backup on 20; 28 drops its copy
        Also:    18 becomes the backup holder for 12's keys (4,12], taking
                 over from 20.
        """
        bootstrap = {"id": None, "url": bootstrap_url.rstrip("/")}
        owner = self.call(bootstrap, "/lookup", {"key": self.id}, request_id)["owner"]
        if owner["id"] == self.id:
            raise PeerError(f"peer ID {self.id} is already live in the ring")

        o_state = self.state_of(owner, request_id)
        pred = o_state["predecessor"] or owner
        succ_of_owner = o_state["successor"]
        pred_of_pred = self.state_of(pred, request_id)["predecessor"] or pred

        change_id = f"join-{self.id}-{request_id}"
        self.pause_ring_from(owner, change_id, True, request_id)
        try:
            with self.lock:
                self.status = "transferring"
                self.predecessor = pred
                self.successors = [owner, succ_of_owner]

            # 1. Copy my new primary range (P, me] from the owner.  The owner keeps its copy until I've confirmed.
            self.move(owner, self.me, pred["id"], self.id, "primary", "primary",
                      request_id)
            # 2. I'm the new backup holder for my predecessor's range.
            if pred["id"] != owner["id"]:
                self.move(owner, self.me, pred_of_pred["id"], pred["id"],
                          "backup", "backup", request_id)

            # 3. Link me into the ring.
            self.call(pred, "/internal/set_successor", {"peer": self.me}, request_id)
            self.call(owner, "/internal/set_predecessor", {"peer": self.me}, request_id)

            # 4. Only now clean up the old copies (handoff was confirmed above).
            self.call(owner, "/internal/set_role", {"start": pred["id"],
                      "end": self.id, "role": "backup"}, request_id)
            if pred["id"] != owner["id"]:
                self.call(owner, "/internal/drop", {"start": pred_of_pred["id"],
                          "end": pred["id"], "role": "backup"}, request_id)
            if succ_of_owner["id"] not in (owner["id"], self.id):
                self.call(succ_of_owner, "/internal/drop", {"start": pred["id"],
                          "end": self.id, "role": "backup"}, request_id)

            # 5. Everyone rebuilds routing, then writes resume.
            self.refresh_ring(request_id)
            with self.lock:
                self.status = "ready"
        finally:
            self.pause_ring(change_id, False, request_id)
        self.info("JOIN_COMPLETE", predecessor=pred["id"], successor=owner["id"],
                  request_id=request_id)

    def pause_ring_from(self, start, change_id, paused, request_id):
        """Pause every existing peer before we are linked into the ring."""
        for peer in self.walk_ring(start, request_id):
            self.call(peer, "/internal/pause",
                      {"change_id": change_id, "paused": paused}, request_id)
        with self.lock:
            self.pause_id = change_id if paused else None

    # Graceful leave
  
    def leave(self, request_id):
        """Hand my data back and unlink myself.

        Example: peer 18 leaves [4, 12, 18, 20, 28].
          P = 12 (predecessor), O = 20 (successor), S = 28 (O's successor)
          (12,18] primary 18 -> primary on 20, backup on 28
          backups of (4,12] held by 18 -> backups on 20
        """
        pred, successors, _, _, _ = self.snapshot()
        owner = successors[0]
        if owner["id"] == self.id:
            raise PeerError("the last peer cannot leave the ring")
        succ_of_owner = self.state_of(owner, request_id)["successor"]
        if succ_of_owner["id"] == self.id:      # two-peer ring
            succ_of_owner = owner
        pred_of_pred = self.state_of(pred, request_id)["predecessor"] or pred

        change_id = f"leave-{self.id}-{request_id}"
        self.pause_ring(change_id, True, request_id)
        with self.lock:
            self.status = "transferring"
        try:
            # 1. My primaries go to my successor 
            self.move(self.me, owner, pred["id"], self.id, "primary", "primary",
                      request_id)
            # 2. get a backup on the successor's successor.
            if succ_of_owner["id"] != owner["id"]:
                self.move(self.me, succ_of_owner, pred["id"], self.id,
                          "primary", "backup", request_id)
            # 3. The backups I held for my predecessor go to my successor.
            if pred["id"] != owner["id"]:
                self.move(self.me, owner, pred_of_pred["id"], pred["id"],
                          "backup", "backup", request_id)

            # 4. Unlink: predecessor <-> successor.
            self.call(pred, "/internal/set_successor", {"peer": owner}, request_id)
            self.call(owner, "/internal/set_predecessor", {"peer": pred}, request_id)

            # 5. Everything confirmed, so my local copies can go.
            self.storage.drop_interval(self.id, self.id)

            # 6. The remaining peers rebuild routing, then resume writes.
            with self.lock:
                self.successors = [owner, succ_of_owner]
            for peer in self.walk_ring(owner, request_id):
                self.call(peer, "/internal/refresh", {}, request_id)
        finally:
            for peer in self.walk_ring(owner, request_id):
                self.call(peer, "/internal/pause",
                          {"change_id": change_id, "paused": False}, request_id)
        self.info("LEAVE_COMPLETE", handed_to=owner["id"], request_id=request_id)

  
    # Crash recovery (one peer fails without leaving)
    
    def watch_predecessor(self):
        """Background loop: if my predecessor stops answering, take over."""
        while True:
            time.sleep(HEALTH_INTERVAL)
            pred, _, _, status, pause_id = self.snapshot()
            if status != "ready" or pause_id or not pred or pred["id"] == self.id:
                self.pred_failures = 0
                continue
            try:
                requests.get(pred["url"] + "/health", timeout=1.0)
                self.pred_failures = 0
            except requests.RequestException:
                self.pred_failures += 1
                self.info("PREDECESSOR_UNREACHABLE", peer=pred["id"],
                          failures=self.pred_failures)
                if self.pred_failures >= FAILURES_BEFORE_RECOVERY:
                    self.pred_failures = 0
                    try:
                        self.recover(pred, str(uuid.uuid4()))
                    except (PeerUnreachable, PeerError) as exc:
                        self.info("RECOVERY_FAILED", error=exc)
                        with self.lock:
                            self.status = "ready"

    def recover(self, dead, request_id):
        """My predecessor crashed.  I hold its backups, so I take over.

        Example: 20 crashes in [4, 12, 20, 28].  28 holds backups of (12,20].
          1. 28 promotes those backups to primary -> 28 now owns (12,28]
          2. 28 finds the peer whose successor was 20 (that's 12) and links
          3. 28 copies its new primaries to its successor 4 as backups
          4. 12's own backups were on 20, so 12 re-sends them to 28
        """
        with self.lock:
            self.status = "recovering"
        self.info("RECOVERY_START", dead=dead["id"], request_id=request_id)

        # Find the live peer whose successor was the dead peer.
        _, successors, _, _, _ = self.snapshot()
        new_pred, current = self.me, successors[0]
        for _ in range(RING_SIZE):
            if current["id"] in (self.id, dead["id"]):
                break
            nxt = self.state_of(current, request_id)["successor"]
            if nxt["id"] == dead["id"]:
                new_pred = current
                break
            current = nxt

        # 1. Promote the backups I held for the dead peer.
        self.storage.promote_backups_to_primary()
        # 2. Link around the dead peer.
        with self.lock:
            self.predecessor = new_pred
            if self.successors[0]["id"] == dead["id"]:
                self.successors = [self.me, self.me]
        if new_pred["id"] != self.id:
            self.call(new_pred, "/internal/set_successor", {"peer": self.me},
                      request_id)
        # 3. Restore backups of everything I now own on my successor.
        _, successors, _, _, _ = self.snapshot()
        if successors[0]["id"] != self.id:
            self.move(self.me, successors[0], new_pred["id"], self.id,
                      "primary", "backup", request_id)
        # 4. My new predecessor's backups lived on the dead peer: re-send them.
        if new_pred["id"] != self.id:
            self.call(new_pred, "/internal/rebackup", {}, request_id)

        with self.lock:
            self.status = "ready"
        self.refresh_ring(request_id)
        self.info("RECOVERY_COMPLETE", dead=dead["id"], new_predecessor=new_pred["id"],
                  request_id=request_id)


# HTTP layer

app = Flask(__name__)
PEER = None   # the ChordPeer for this process, created in main()


def _in_request():
    from flask import has_request_context
    return has_request_context()


def ok(**body):
    return jsonify({"success": True, **body}), 200


def fail(message, status):
    PEER.info("ERROR_REPLY", status=status, error=message)
    return jsonify({"success": False, "error": message}), status


def body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


@app.before_request
def log_request():
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    PEER.info("RECV", method=request.method, path=request.path)


@app.after_request
def add_request_id(response):
    response.headers["X-Request-ID"] = g.request_id
    return response


#public API
@app.get("/health")
def health():
    _, _, _, status, pause_id = PEER.snapshot()
    shown = "transferring" if pause_id and status == "ready" else status
    return ok(peer_id=PEER.id, status=shown)


@app.get("/state")
def state():
    pred, successors, fingers, status, pause_id = PEER.snapshot()
    return ok(peer_id=PEER.id, url=PEER.me["url"], predecessor=pred,
              successor=successors[0], successors=successors, fingers=fingers,
              status=status, ready=status == "ready" and not pause_id,
              records=PEER.storage.counts())


@app.post("/lookup")
def lookup():
    data = body()
    key, hops, path = data.get("key"), data.get("hops", 0), data.get("path", [])
    if not valid_key(key):
        return fail("key must be an integer in [0, 31]", 400)
    if type(hops) is not int or not isinstance(path, list):
        return fail("hops must be an int and path a list", 400)
    try:
        owner, hops, path = PEER.lookup(key, hops, path, g.request_id)
    except PeerError as exc:
        return fail(str(exc), 503)
    return ok(owner=owner, hops=hops, path=path)


@app.post("/join")
def join_route():
    url = body().get("bootstrap_url")
    if not isinstance(url, str):
        return fail("bootstrap_url is required", 400)
    try:
        PEER.join(url, g.request_id)
    except (PeerUnreachable, PeerError) as exc:
        return fail(str(exc), 409 if "already live" in str(exc) else 503)
    return ok(peer_id=PEER.id, status="ready")


@app.post("/leave")
def leave_route():
    try:
        PEER.leave(g.request_id)
    except (PeerUnreachable, PeerError) as exc:
        return fail(str(exc), 503)
    # Reply first, then stop this process half a second later.
    threading.Timer(0.5, PEER.server.shutdown).start()
    return ok(peer_id=PEER.id, status="left")


# records
def check_record(data):
    """Validate record_id/routing_key and that I currently own the key."""
    record_id, key = data.get("record_id"), data.get("routing_key")
    if not valid_key(key):
        return None, None, fail("routing_key must be an integer in [0, 31]", 400)
    expected = routing_key_for(record_id)
    if expected is None:
        return None, None, fail("record_id must be post:<n>, author:<n> or "
                                "meta:next_post_id", 400)
    if expected != key:
        return None, None, fail("routing_key does not match the record ID hash", 400)
    pred, successors, _, _, _ = PEER.snapshot()
    owns = successors[0]["id"] == PEER.id or \
        (pred is not None and in_open_closed(key, pred["id"], PEER.id))
    if not owns:
        return None, None, fail(f"peer {PEER.id} does not own key {key}; "
                                "repeat the lookup", 503)
    return record_id, key, None


def replicate(record, request_id):
    """Send one record (live or tombstone) to my successor as its backup."""
    successor = PEER.snapshot()[1][0]
    PEER.call(successor, "/internal/import",
              {"records": [record], "role": "backup"}, request_id)
    return successor


@app.post("/records/get")
def records_get():
    record_id, key, error = check_record(body())
    if error:
        return error
    stored = PEER.storage.get(record_id)
    found = stored is not None and not stored["deleted"]
    PEER.info("RECORD_GET", record=record_id, key=key, found=found)
    if not found:
        return ok(found=False)
    return ok(found=True, record=stored["value"])


@app.post("/records/put")
def records_put():
    data = body()
    record_id, key, error = check_record(data)
    if error:
        return error
    allowed, reason = PEER.writes_allowed()
    if not allowed:
        return fail(reason, 503)
    value = data.get("value")
    if not isinstance(value, dict):
        return fail("value must be a JSON object", 400)
    if record_id.startswith("post:") and value.get("id") != int(record_id[5:]):
        return fail("value must be the post object with a matching id", 400)

    # Primary first, then the backup on my successor. Success only if both.
    stored = PEER.storage.put(record_id, key, value, data.get("operation_id"))
    try:
        successor = replicate(stored, g.request_id)
    except (PeerUnreachable, PeerError) as exc:
        return fail(f"primary stored but backup not confirmed: {exc}", 503)
    PEER.info("RECORD_PUT", record=record_id, key=key, version=stored["version"],
              backup_on=successor["id"], operation_id=data.get("operation_id"))
    return ok(stored=True, role="primary", backup=successor["id"],
              version=stored["version"])


@app.post("/records/delete")
def records_delete():
    data = body()
    record_id, key, error = check_record(data)
    if error:
        return error
    allowed, reason = PEER.writes_allowed()
    if not allowed:
        return fail(reason, 503)
    tombstone = PEER.storage.delete(record_id, key, data.get("operation_id"))
    try:
        successor = replicate(tombstone, g.request_id)
    except (PeerUnreachable, PeerError) as exc:
        return fail(f"primary deleted but backup not confirmed: {exc}", 503)
    PEER.info("RECORD_DELETE", record=record_id, key=key, version=tombstone["version"],
              backup_on=successor["id"], operation_id=data.get("operation_id"))
    return ok(deleted=True, version=tombstone["version"])


#internal
@app.post("/internal/pause")
def internal_pause():
    data = body()
    change_id, paused = data.get("change_id"), data.get("paused")
    with PEER.lock:
        if paused:
            if PEER.pause_id not in (None, change_id):
                return fail(f"membership change {PEER.pause_id} in progress", 409)
            PEER.pause_id = change_id
        elif PEER.pause_id == change_id:
            PEER.pause_id = None
    PEER.info("WRITES_PAUSED" if paused else "WRITES_RESUMED", change=change_id)
    return ok(paused=bool(paused))


@app.post("/internal/set_successor")
def internal_set_successor():
    peer = body().get("peer")
    with PEER.lock:
        PEER.successors = [peer, PEER.successors[-1]]
    PEER.info("SUCCESSOR_SET", successor=peer["id"])
    return ok()


@app.post("/internal/set_predecessor")
def internal_set_predecessor():
    peer = body().get("peer")
    with PEER.lock:
        PEER.predecessor = peer
    PEER.info("PREDECESSOR_SET", predecessor=peer["id"])
    return ok()


@app.post("/internal/export")
def internal_export():
    data = body()
    return ok(records=PEER.storage.list_interval(data["start"], data["end"],
                                                 data["role"]))


@app.post("/internal/import")
def internal_import():
    data = body()
    applied, ignored = PEER.storage.import_records(data["records"], data["role"])
    PEER.info("IMPORTED", count=len(data["records"]), role=data["role"],
              applied=applied, ignored=ignored)
    # "imported" confirms every record is now safely held: applied, or we
    # already had the same or a newer version.
    return ok(imported=len(data["records"]), applied=applied, ignored=ignored)


@app.post("/internal/set_role")
def internal_set_role():
    data = body()
    PEER.storage.set_role_for_interval(data["start"], data["end"], data["role"])
    PEER.info("ROLE_SET", interval=f"({data['start']},{data['end']}]",
              role=data["role"])
    return ok()


@app.post("/internal/drop")
def internal_drop():
    data = body()
    dropped = PEER.storage.drop_interval(data["start"], data["end"], data["role"])
    PEER.info("DROPPED", interval=f"({data['start']},{data['end']}]",
              role=data["role"], count=dropped)
    return ok(dropped=dropped)


@app.post("/internal/drop_ids")
def internal_drop_ids():
    PEER.storage.drop_ids(body().get("ids", []))
    return ok()


@app.post("/internal/refresh")
def internal_refresh():
    try:
        PEER.refresh(g.request_id)
    except (PeerUnreachable, PeerError) as exc:
        return fail(str(exc), 503)
    return ok()


@app.post("/internal/rebackup")
def internal_rebackup():
    """Re-send all my primaries to my successor as backups (after a crash)."""
    pred, successors, _, _, _ = PEER.snapshot()
    start = pred["id"] if pred else PEER.id
    count = PEER.move(PEER.me, successors[0], start, PEER.id, "primary", "backup",
                      g.request_id)
    return ok(count=count)


# Startup

def main():
    global PEER
    parser = argparse.ArgumentParser(description="Chord storage peer")
    parser.add_argument("--id", type=int, required=True, help="ring ID 0-31")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--db-dir", default="peer_data",
                        help="folder for this peer's own peer_<id>.db")
    parser.add_argument("--bootstrap", help="URL of any live peer to join through")
    args = parser.parse_args()
    if not valid_key(args.id):
        parser.error("--id must be in [0, 31]")

    PEER = ChordPeer(args.id, f"http://{args.host}:{args.port}", args.db_dir)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    PEER.server = make_server(args.host, args.port, app, threaded=True)
    http_thread = threading.Thread(target=PEER.server.serve_forever, daemon=True)
    http_thread.start()
    PEER.info("HTTP_LISTEN", host=args.host, port=args.port)

    request_id = str(uuid.uuid4())
    if args.bootstrap:
        try:
            PEER.join(args.bootstrap, request_id)
        except (PeerUnreachable, PeerError) as exc:
            PEER.info("JOIN_FAILED", error=exc)
            PEER.server.shutdown()
            sys.exit(1)
    else:
        # First peer: a one-peer ring owns every key.
        with PEER.lock:
            PEER.predecessor = PEER.me
            PEER.status = "ready"
        PEER.refresh(request_id)
        PEER.info("RING_CREATED")

    threading.Thread(target=PEER.watch_predecessor, daemon=True).start()
    try:
        # serve_forever returns after /leave calls server.shutdown().
        while http_thread.is_alive():
            http_thread.join(0.5)
    except KeyboardInterrupt:
        PEER.server.shutdown()
    PEER.server.server_close()
    PEER.info("STOPPED")


if __name__ == "__main__":
    main()
