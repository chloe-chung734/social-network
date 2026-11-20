"""coordination_state.py - persistent lock-state schema (Member 3).

The coordinator keeps ONE JSON document in ZooKeeper at
    <COORD_ROOT>/locks/post-write/state        (default root /social-network/m4)
It is the single source of truth for the centralized mutual-exclusion lock.
Every change is written with a ZooKeeper transaction that also checks the
coordinator's election authority (see coordinator_server.py), so this module
contains only PURE functions: read a state dict, return a new one.

Shared functions (used by other members):
    initial_state() -> dict          Member 2's election creates the znode with it
    grant_matches(state, grant)      Member 5 / Post: "is this grant the holder?"

STATE SCHEMA (schema_version 1)
-------------------------------
{
  "schema_version": 1,
  "resource": "post-write",
  "holder": null | GRANT,               # who is in the critical section now
  "next_token": 1,                      # next grant token; only ever increases
  "queue": [ {"owner_id", "request_id"}, ... ],   # durable FIFO of waiters
  "requests": { KEY: REQUEST, ... },    # every live request + recent finished ones
  "completed": [ KEY, ... ],            # finished keys, oldest first (pruning order)
  "vector_clock": { node_id: counter }, # element-wise max of every vector recorded
  "history": [ HISTORY_ENTRY, ... ],    # last MAX_HISTORY state changes, oldest first
  "updated_by": null | {"node_id", "coordinator_generation"}
}

KEY      = owner_id + " " + request_id   (neither may contain a space; see request_key)
GRANT    = {"resource", "owner_id", "request_id", "grant_token", "coordinator_generation"}
REQUEST  = {"owner_id", "request_id",
            "status": "queued" | "granted" | "released" | "cancelled",
            "grant_token": int | null,     # set when granted, kept after release
            "seq": int}                    # arrival order, for diagnostics
HISTORY_ENTRY = {"event", "owner_id", "request_id", "grant_token",
                 "coordinator", "coordinator_generation", "vector_clock"}

Invariants (checked by validate_state):
  * holder is null, or requests[holder key] has status "granted" with the same token
  * at most one request has status "granted" (mutual exclusion)
  * every queue entry has status "queued", and every queued request is in the queue once
  * if holder is null the queue is empty (a release always grants the next waiter)
  * every grant_token < next_token
Finished requests ("released"/"cancelled") are kept so duplicate releases are
recognised; only the oldest beyond MAX_COMPLETED are pruned.
"""

import copy

SCHEMA_VERSION = 1
RESOURCE = "post-write"
MAX_HISTORY = 100
MAX_COMPLETED = 500

QUEUED, GRANTED, RELEASED, CANCELLED = "queued", "granted", "released", "cancelled"


class StateError(ValueError):
    """The stored state breaks the schema or its invariants."""


# --------------------------------------------------------------- shared API

def initial_state():
    """A fresh, empty lock state. Only used when the znode does not exist yet."""
    return {
        "schema_version": SCHEMA_VERSION,
        "resource": RESOURCE,
        "holder": None,
        "next_token": 1,
        "queue": [],
        "requests": {},
        "completed": [],
        "vector_clock": {},
        "history": [],
        "updated_by": None,
    }


def grant_matches(state, grant):
    """True only if `grant` is the CURRENT holder's grant (owner, request, token).

    Accepts the grant object returned by /lock/acquire. A released, replaced,
    or forged grant returns False. Never raises on bad input.
    """
    if not isinstance(state, dict) or not isinstance(grant, dict):
        return False
    holder = state.get("holder")
    if not isinstance(holder, dict):
        return False
    # Persisted state uses ``grant_token``; the Milestone 4 HTTP grant uses
    # ``token``. Accept both forms at this shared validation boundary.
    token = grant.get("token", grant.get("grant_token"))
    return (type(token) is int
            and token == holder.get("grant_token")
            and grant.get("owner_id") == holder.get("owner_id")
            and grant.get("request_id") == holder.get("request_id")
            and grant.get("resource", RESOURCE) == holder.get("resource", RESOURCE))


def request_key(owner_id, request_id):
    """Identity of one lock request. Space is safe: IDs never contain one."""
    for name, value in (("owner_id", owner_id), ("request_id", request_id)):
        if not isinstance(value, str) or not value or " " in value or len(value) > 300:
            raise ValueError(f"{name} must be a non-empty string without spaces")
    return f"{owner_id} {request_id}"


def queue_position(state, owner_id, request_id):
    """1-based position in the FIFO queue, or None if not queued."""
    for i, entry in enumerate(state["queue"], 1):
        if entry["owner_id"] == owner_id and entry["request_id"] == request_id:
            return i
    return None


def validate_state(state):
    """Raise StateError if the state breaks the schema or invariants."""
    if not isinstance(state, dict) or state.get("schema_version") != SCHEMA_VERSION:
        raise StateError("unknown lock-state schema version")
    for field in ("queue", "completed", "history"):
        if not isinstance(state.get(field), list):
            raise StateError(f"{field} must be a list")
    for field in ("requests", "vector_clock"):
        if not isinstance(state.get(field), dict):
            raise StateError(f"{field} must be an object")
    if type(state.get("next_token")) is not int or state["next_token"] < 1:
        raise StateError("next_token must be a positive integer")
    reqs = state["requests"]
    granted = [k for k, r in reqs.items() if r.get("status") == GRANTED]
    if len(granted) > 1:
        raise StateError(f"more than one granted request: {granted}")
    holder = state.get("holder")
    if holder is None:
        if granted:
            raise StateError("a request is granted but holder is null")
        if state["queue"]:
            raise StateError("queue is non-empty while nobody holds the lock")
    else:
        key = request_key(holder["owner_id"], holder["request_id"])
        rec = reqs.get(key)
        if not rec or rec.get("status") != GRANTED or rec.get("grant_token") != holder.get("grant_token"):
            raise StateError("holder does not match its request record")
    queued_keys = [request_key(e["owner_id"], e["request_id"]) for e in state["queue"]]
    if len(set(queued_keys)) != len(queued_keys):
        raise StateError("a request appears twice in the queue")
    if any(reqs.get(k, {}).get("status") != QUEUED for k in queued_keys):
        raise StateError("queue entry whose request is not queued")
    if {k for k, r in reqs.items() if r.get("status") == QUEUED} != set(queued_keys):
        raise StateError("queued request missing from the queue")
    for r in reqs.values():
        tok = r.get("grant_token")
        if tok is not None and (type(tok) is not int or tok >= state["next_token"]):
            raise StateError("grant_token not below next_token")
    return state


def merge_vectors(a, b):
    """Element-wise maximum of two vector clocks."""
    out = dict(a or {})
    for node, count in (b or {}).items():
        if type(count) is int and count > out.get(node, 0):
            out[node] = count
    return out


# ------------------------------------------------------------- transitions
# Each transition takes the CURRENT stored state and returns
# (new_state_or_None, result). new_state is None when nothing changes, so the
# caller writes nothing (idempotent retries cost no ZooKeeper write).
# `ctx` = {"node_id", "coordinator_generation", "vector_clock"} describing the
# coordinator event that causes the change.

def _record(state, event, key_info, token, ctx):
    entry = {
        "event": event,
        "owner_id": key_info["owner_id"],
        "request_id": key_info["request_id"],
        "grant_token": token,
        "coordinator": ctx["node_id"],
        "coordinator_generation": ctx["coordinator_generation"],
        "vector_clock": ctx["vector_clock"],
    }
    state["history"] = (state["history"] + [entry])[-MAX_HISTORY:]
    state["vector_clock"] = merge_vectors(state["vector_clock"], ctx["vector_clock"])
    state["updated_by"] = {"node_id": ctx["node_id"],
                           "coordinator_generation": ctx["coordinator_generation"]}


def _grant(state, rec, ctx):
    token = state["next_token"]
    state["next_token"] = token + 1
    rec["status"] = GRANTED
    rec["grant_token"] = token
    grant = {"resource": state["resource"], "owner_id": rec["owner_id"],
             "request_id": rec["request_id"], "grant_token": token,
             "coordinator_generation": ctx["coordinator_generation"]}
    state["holder"] = grant
    _record(state, "LOCK_GRANTED", rec, token, ctx)
    return grant


def _finish(state, key, status):
    state["requests"][key]["status"] = status
    state["completed"].append(key)
    while len(state["completed"]) > MAX_COMPLETED:
        old = state["completed"].pop(0)
        if state["requests"].get(old, {}).get("status") in (RELEASED, CANCELLED):
            del state["requests"][old]


def apply_acquire(state, owner_id, request_id, ctx):
    """Acquire or re-poll. Returns (new_state|None, result).

    result["outcome"]:
      "granted"   - this request holds the lock; result["grant"] is the grant
      "queued"    - waiting; result["position"] is its 1-based FIFO position
      "finished"  - this request was already released/cancelled (use a new request_id)
    A retry of a known request never enqueues twice and never makes a new grant.
    """
    key = request_key(owner_id, request_id)
    rec = state["requests"].get(key)
    if rec is not None:                              # retry / poll: no change
        if rec["status"] == GRANTED:
            return None, {"outcome": "granted", "grant": copy.deepcopy(state["holder"]),
                          "duplicate": True}
        if rec["status"] == QUEUED:
            return None, {"outcome": "queued", "duplicate": True,
                          "position": queue_position(state, owner_id, request_id),
                          "holder": copy.deepcopy(state["holder"])}
        return None, {"outcome": "finished", "status": rec["status"],
                      "grant_token": rec.get("grant_token")}

    new = copy.deepcopy(state)
    seq = max((r.get("seq", 0) for r in new["requests"].values()), default=0) + 1
    rec = {"owner_id": owner_id, "request_id": request_id, "status": QUEUED,
           "grant_token": None, "seq": seq}
    new["requests"][key] = rec
    if new["holder"] is None and not new["queue"]:
        grant = _grant(new, rec, ctx)
        return new, {"outcome": "granted", "grant": copy.deepcopy(grant), "duplicate": False}
    new["queue"].append({"owner_id": owner_id, "request_id": request_id})
    _record(new, "LOCK_QUEUED", rec, None, ctx)
    return new, {"outcome": "queued", "duplicate": False,
                 "position": len(new["queue"]), "holder": copy.deepcopy(new["holder"])}


def apply_release(state, owner_id, request_id, grant_token, ctx):
    """Release (or cancel a waiting request). Returns (new_state|None, result).

    result["outcome"]:
      "released"   - holder released; result["next_grant"] is who got the lock next (or None)
      "duplicate"  - this exact grant was already released; nothing changes
      "cancelled"  - a still-queued request was withdrawn (grant_token must be null)
      "stale"      - token/owner/request does not match; nothing changes
    """
    key = request_key(owner_id, request_id)
    rec = state["requests"].get(key)
    if rec is None:
        return None, {"outcome": "stale", "reason": "unknown request"}
    status = rec["status"]

    if status == QUEUED:
        if grant_token is not None:
            return None, {"outcome": "stale", "reason": "request is queued and has no grant"}
        new = copy.deepcopy(state)
        new["queue"] = [e for e in new["queue"]
                        if not (e["owner_id"] == owner_id and e["request_id"] == request_id)]
        _finish(new, key, CANCELLED)
        _record(new, "LOCK_CANCELLED", rec, None, ctx)
        return new, {"outcome": "cancelled"}

    if type(grant_token) is not int or grant_token != rec.get("grant_token"):
        return None, {"outcome": "stale", "reason": "grant_token does not match this request"}
    if status == RELEASED:
        return None, {"outcome": "duplicate", "grant_token": grant_token}
    if status != GRANTED or not grant_matches(state, {"owner_id": owner_id,
                                                      "request_id": request_id,
                                                      "grant_token": grant_token}):
        return None, {"outcome": "stale", "reason": f"request is {status}"}

    new = copy.deepcopy(state)
    _finish(new, key, RELEASED)
    new["holder"] = None
    _record(new, "LOCK_RELEASED", rec, grant_token, ctx)
    next_grant = None
    if new["queue"]:                       # FIFO hand-off, in the same atomic write
        head = new["queue"].pop(0)
        nxt = new["requests"][request_key(head["owner_id"], head["request_id"])]
        next_grant = copy.deepcopy(_grant(new, nxt, ctx))
    return new, {"outcome": "released", "grant_token": grant_token, "next_grant": next_grant}


def status_view(state):
    """Summary for GET /lock/status (no full request table)."""
    return {
        "resource": state["resource"],
        "holder": copy.deepcopy(state["holder"]),
        "queue": copy.deepcopy(state["queue"]),
        "next_token": state["next_token"],
        "vector_clock": dict(state["vector_clock"]),
        "updated_by": copy.deepcopy(state["updated_by"]),
        "recent_history": copy.deepcopy(state["history"][-10:]),
    }
