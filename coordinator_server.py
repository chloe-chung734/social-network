"""coordinator_server.py - centralized mutual-exclusion coordinator (Member 3).

Run three candidates; Member 2's ZooKeeper election picks ONE leader, and only
the leader changes lock state:

    python coordinator_server.py --name coord-1 --port 5101
    python coordinator_server.py --name coord-2 --port 5102
    python coordinator_server.py --name coord-3 --port 5103

Options: --host (127.0.0.1), --zk-hosts (env ZK_HOSTS, default 127.0.0.1:2181),
--zk-root (env COORD_ROOT, default /social-network/m4), --session-timeout (10 s).

HTTP API (JSON; every request/response carries Member 1's X-Node-ID,
X-Request-ID and X-Vector-Clock headers)
-----------------------------------------------------------------------
POST /lock/acquire  {"resource": "post-write", "owner_id": "...", "request_id": "..."}
    owner_id defaults to the X-Node-ID header, request_id to X-Request-ID.
    200 {"success": true, "status": "granted", "grant": GRANT, ...}
    202 {"success": true, "status": "queued", "position": N, "holder": GRANT}
    409 {"success": false, "error": "request_finished", "request_status": ...}  (finished id reused)
    Re-sending the same (owner_id, request_id) is a POLL: it never enqueues twice
    and never creates a second grant. Waiters poll until they get 200.
POST /lock/release  {"resource": "post-write", "owner_id", "request_id", "grant_token"}
    200 {"status": "released", "next_grant": GRANT|null}   hand-off is in the same write
    200 {"status": "released", "duplicate": true}          same grant released before
    200 {"status": "cancelled"}                            queued request withdrawn
                                                           (send "grant_token": null)
    409 {"error": "stale_release", "reason": ...}          token/owner/request mismatch
GET  /lock/status   200 {"is_leader", "coordinator", "leader", "state": {...}}
                    (served by any candidate; read-only)
GET  /health
Non-leader or unverifiable leadership on acquire/release:
    503 {"success": false, "error": "not_leader", "leader": {"node_id", "url"}|null}
Other errors: 400 bad input/metadata, 503 "coordination_unavailable".

SAFETY RULES
  * Every state change is ONE ZooKeeper transaction: check(own candidate znode,
    version) + set_data(lock state, expected version). A deposed leader or a
    concurrent change makes the whole transaction fail, so nothing is applied.
  * A grant is returned only after its transaction committed (persisted).
  * An ambiguous commit (connection loss) is never retried blindly: the state
    is re-read and the transition recomputed. Transitions are idempotent per
    (owner_id, request_id), so an applied grant is found, not issued twice.
  * Conservative failure behaviour (contract): no lock expiry, no clearing of
    state on startup, no automatic recovery when a writer crashes. A grant
    stays valid until its matching release, across leader changes.
"""

import argparse
import logging
import os
import threading
import time
import uuid

from flask import Flask, g, jsonify, request
from kazoo.exceptions import KazooException
from werkzeug.serving import make_server

import coordination_state as cs
from message_protocol import MessageContext, MessageProtocolError
from zk_coordination import (CoordinationUnavailable, NotLeaderError,
                             ZKCoordination, encode_state)

log = logging.getLogger("coordinator")

ACQUIRE_PATH = "/lock/acquire"
RELEASE_PATH = "/lock/release"
STATUS_PATH = "/lock/status"
MAX_ATTEMPTS = 8            # optimistic-concurrency retries per request
RECOVERY_POLL = 0.5         # seconds between leadership checks for recovery


class LockCoordinator:
    """All lock logic; HTTP-free so tests can call it directly."""

    def __init__(self, ctx, zk):
        self.ctx = ctx
        self.zk = zk
        self._mutex = threading.Lock()        # one state change at a time per process
        self._recovered_generation = None
        self._stop = threading.Event()
        self._watcher = None

    # ------------------------------------------------------------ lifecycle
    def start_recovery_watcher(self):
        """Recover as soon as this node wins an election (visible in the logs)."""
        def loop():
            while not self._stop.wait(RECOVERY_POLL):
                try:
                    tok = self.zk.leadership_token()
                except CoordinationUnavailable:
                    continue
                try:
                    with self._mutex:
                        self._recover_if_needed(tok)
                except (CoordinationUnavailable, KazooException, cs.StateError) as exc:
                    log.warning("recovery check failed: %s", exc)
        self._watcher = threading.Thread(target=loop, name="lock-recovery", daemon=True)
        self._watcher.start()

    def stop(self):
        self._stop.set()

    # -------------------------------------------------------------- recovery
    def _recover_if_needed(self, tok):
        """Once per leadership term: adopt the stored state, merge its vectors."""
        gen = tok["generation"]
        if self._recovered_generation == gen:
            return
        state, version = self.zk.read_lock_state()
        cs.validate_state(state)
        stored = state.get("vector_clock") or {}
        if stored:
            self.ctx.clock.receive(stored)     # merge recovered vector history
        prev = state.get("updated_by") or {}
        self.ctx.local_event(
            "STATE_RECOVERED", f"recover:{gen}", coordinator_generation=gen,
            previous_coordinator=prev.get("node_id"),
            previous_generation=prev.get("coordinator_generation"),
            holder=state["holder"], queue_length=len(state["queue"]),
            next_token=state["next_token"], zk_version=version,
            recovered_vector=stored)
        self._recovered_generation = gen

    # ------------------------------------------------------------- mutation
    def _mutate(self, transition, request_id):
        """Run transition(state, ctx) under authority + version checks.

        Returns (result, committed_vector_or_None, ambiguous_seen).
        Raises NotLeaderError / CoordinationUnavailable.
        """
        ambiguous = False
        with self._mutex:
            for _ in range(MAX_ATTEMPTS):
                tok = self.zk.leadership_token()            # raises NotLeaderError
                self._recover_if_needed(tok)
                state, version = self.zk.read_lock_state()
                cs.validate_state(state)
                info = {"node_id": self.ctx.node_id,
                        "coordinator_generation": tok["generation"], "vector_clock": {}}
                new, result = transition(state, info)
                if new is None:                              # idempotent: nothing to write
                    return result, None, ambiguous
                info["vector_clock"] = self.ctx.clock.tick()  # the decision event
                new, result = transition(state, info)
                cs.validate_state(new)
                try:
                    txn = self.zk.leader_txn()               # adds check(candidate, version)
                    txn.set_data(self.zk.lock_state_path, encode_state(new), version=version)
                    results = txn.commit()
                except NotLeaderError:
                    raise
                except KazooException as exc:                # outcome unknown: reread, recompute
                    ambiguous = True
                    self.ctx.log("LOCK_COMMIT_UNKNOWN", request_id, error=type(exc).__name__,
                                 zk_version=version)
                    self._wait_connected()
                    continue
                if not any(isinstance(r, Exception) for r in results):
                    return result, info["vector_clock"], ambiguous
                if isinstance(results[0], Exception):        # leadership check failed
                    raise NotLeaderError("leadership check failed at commit")
                # lock-state version changed under us: reread and retry
            raise CoordinationUnavailable("lock state kept changing; try again")

    def _wait_connected(self, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end and not self.zk.client.connected:
            time.sleep(0.1)

    # ---------------------------------------------------------------- public
    def acquire(self, owner_id, request_id, trace_id):
        self.ctx.log("LOCK_REQUEST", trace_id, resource=cs.RESOURCE,
                     owner_id=owner_id, lock_request_id=request_id, grant_token=None)
        result, vec, ambiguous = self._mutate(
            lambda st, info: cs.apply_acquire(st, owner_id, request_id, info), trace_id)
        if ambiguous and result["outcome"] in ("granted", "queued"):
            result["duplicate"] = False          # our own uncertain write, not a client retry
        outcome = result["outcome"]
        fields = dict(resource=cs.RESOURCE, owner_id=owner_id, lock_request_id=request_id,
                      duplicate=result.get("duplicate", False), committed_vector=vec)
        if outcome == "granted":
            self.ctx.log("LOCK_GRANTED", trace_id, grant_token=result["grant"]["grant_token"],
                         coordinator_generation=result["grant"]["coordinator_generation"], **fields)
        elif outcome == "queued":
            self.ctx.log("LOCK_QUEUED", trace_id, position=result["position"],
                         grant_token=None, **fields)
        else:
            self.ctx.log("LOCK_REQUEST_FINISHED", trace_id, status=result["status"], **fields)
        return result

    def release(self, owner_id, request_id, grant_token, trace_id):
        self.ctx.log("LOCK_RELEASE_REQUEST", trace_id, resource=cs.RESOURCE,
                     owner_id=owner_id, lock_request_id=request_id, grant_token=grant_token)
        result, vec, ambiguous = self._mutate(
            lambda st, info: cs.apply_release(st, owner_id, request_id, grant_token, info),
            trace_id)
        if ambiguous and result["outcome"] == "duplicate":
            # our own uncertain commit applied: report it as the release it was
            state, _ = self.zk.read_lock_state()
            result = {"outcome": "released", "grant_token": grant_token,
                      "next_grant": state["holder"], "recovered_after_unknown_commit": True}
        fields = dict(resource=cs.RESOURCE, owner_id=owner_id, lock_request_id=request_id,
                      grant_token=grant_token, committed_vector=vec)
        event = {"released": "LOCK_RELEASED", "duplicate": "LOCK_RELEASE_DUPLICATE",
                 "cancelled": "LOCK_CANCELLED", "stale": "LOCK_RELEASE_STALE"}[result["outcome"]]
        extra = {}
        if result.get("next_grant"):
            extra["next_owner_id"] = result["next_grant"]["owner_id"]
            extra["next_grant_token"] = result["next_grant"]["grant_token"]
        if result["outcome"] == "stale":
            extra["reason"] = result["reason"]
        self.ctx.log(event, trace_id, **fields, **extra)
        return result

    def status(self):
        state, version = self.zk.read_lock_state()
        leader = self.zk.leader_info()
        return {"is_leader": self.zk.is_leader(), "coordinator": self.ctx.node_id,
                "coordinator_generation": self.zk.generation,
                "leader": ({"node_id": leader["node_id"], "url": leader["url"],
                            "generation": leader["generation"]} if leader else None),
                "zk_version": version, "state": cs.status_view(state)}


# ---------------------------------------------------------------- HTTP layer

def create_app(coordinator):
    ctx, zk = coordinator.ctx, coordinator.zk
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 64 * 1024
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    @app.before_request
    def clock_receive():
        try:
            g.request_id = ctx.receive_headers(request.headers)
        except MessageProtocolError as exc:
            g.request_id = str(uuid.uuid4())
            return jsonify({"success": False, "error": "bad_metadata", "detail": str(exc)}), 400

    @app.after_request
    def clock_send(response):
        response.headers.update(ctx.send_headers(g.request_id))
        return response

    def fail(error, status, **extra):
        return jsonify({"success": False, "error": error, **extra}), status

    def not_leader(exc):
        info = None
        try:
            leader = zk.leader_info()
            if leader:
                info = {"node_id": leader["node_id"], "url": leader["url"]}
        except Exception:  # noqa: BLE001 - the hint is optional
            pass
        ctx.log("LOCK_REJECTED_NOT_LEADER", g.request_id, path=request.path,
                reason=str(exc), leader=info)
        return fail("not_leader", 503, detail=str(exc), leader=info)

    def lock_body(need_token):
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            raise ValueError("body must be a JSON object")
        if data.get("resource", cs.RESOURCE) != cs.RESOURCE:
            raise ValueError(f"unknown resource; only {cs.RESOURCE!r} exists")
        owner = data.get("owner_id") or request.headers.get("X-Node-ID")
        rid = data.get("request_id") or g.request_id
        cs.request_key(owner, rid)                    # validates both
        token = data.get("grant_token")
        if need_token and token is not None and type(token) is not int:
            raise ValueError("grant_token must be an integer or null")
        return owner, rid, token

    @app.get("/health")
    def health():
        return jsonify({"success": True, "service": "coordinator",
                        "node_id": ctx.node_id, "is_leader": zk.is_leader()})

    @app.post(ACQUIRE_PATH)
    def acquire():
        try:
            owner, rid, _ = lock_body(False)
        except ValueError as exc:
            return fail("bad_request", 400, detail=str(exc))
        try:
            result = coordinator.acquire(owner, rid, g.request_id)
        except NotLeaderError as exc:
            return not_leader(exc)
        except (CoordinationUnavailable, cs.StateError) as exc:
            return fail("coordination_unavailable", 503, detail=str(exc))
        base = {"coordinator": ctx.node_id, "coordinator_generation": zk.generation,
                "owner_id": owner, "request_id": rid}
        if result["outcome"] == "granted":
            return jsonify({"success": True, "status": "granted", "grant": result["grant"],
                            "duplicate": result["duplicate"], **base}), 200
        if result["outcome"] == "queued":
            return jsonify({"success": True, "status": "queued", "position": result["position"],
                            "holder": result["holder"], "duplicate": result["duplicate"],
                            **base}), 202
        return fail("request_finished", 409, request_status=result["status"],
                    grant_token=result.get("grant_token"),
                    detail="this request_id was already released or cancelled; use a new one",
                    **base)

    @app.post(RELEASE_PATH)
    def release():
        try:
            owner, rid, token = lock_body(True)
        except ValueError as exc:
            return fail("bad_request", 400, detail=str(exc))
        try:
            result = coordinator.release(owner, rid, token, g.request_id)
        except NotLeaderError as exc:
            return not_leader(exc)
        except (CoordinationUnavailable, cs.StateError) as exc:
            return fail("coordination_unavailable", 503, detail=str(exc))
        base = {"coordinator": ctx.node_id, "owner_id": owner, "request_id": rid,
                "grant_token": token}
        outcome = result["outcome"]
        if outcome == "released":
            return jsonify({"success": True, "status": "released", "duplicate": False,
                            "next_grant": result["next_grant"], **base}), 200
        if outcome == "duplicate":
            return jsonify({"success": True, "status": "released", "duplicate": True,
                            **base}), 200
        if outcome == "cancelled":
            return jsonify({"success": True, "status": "cancelled", **base}), 200
        return fail("stale_release", 409, reason=result["reason"], **base)

    @app.get(STATUS_PATH)
    def status():
        try:
            return jsonify({"success": True, **coordinator.status()})
        except CoordinationUnavailable as exc:
            return fail("coordination_unavailable", 503, detail=str(exc))

    return app


def main(argv=None):
    p = argparse.ArgumentParser(description="Lock coordinator candidate")
    p.add_argument("--name", required=True, help="coord-1, coord-2 or coord-3")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--zk-hosts", default=os.getenv("ZK_HOSTS", "127.0.0.1:2181"))
    p.add_argument("--zk-root", default=os.getenv("COORD_ROOT", "/social-network/m4"))
    p.add_argument("--session-timeout", type=float, default=10.0)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")

    ctx = MessageContext(args.name)
    url = f"http://{args.host}:{args.port}"
    zk = ZKCoordination(ctx.node_id, ctx, hosts=args.zk_hosts, root=args.zk_root,
                        advertised_url=url, session_timeout=args.session_timeout)
    coordinator = LockCoordinator(ctx, zk)
    server = make_server(args.host, args.port, create_app(coordinator), threaded=True)
    http = threading.Thread(target=server.serve_forever, daemon=True)
    http.start()                                  # HTTP first; requests fail 503 until leader
    log.info("%s listening on %s (clock log %s)", ctx.node_id, url, ctx.log_path)
    zk.start()                                    # joins the election; never clears state
    coordinator.start_recovery_watcher()
    try:
        while http.is_alive():
            http.join(0.5)
    except KeyboardInterrupt:
        log.info("stopping %s", args.name)
    finally:
        coordinator.stop()
        server.shutdown()
        zk.close()                                # resign so the next candidate takes over


if __name__ == "__main__":
    main()
