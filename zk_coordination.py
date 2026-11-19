"""zk_coordination.py - ZooKeeper stuff for the coordinator (Member 2)

What it does: everybody registers under /members, and the coordinator
candidates run an election to pick one leader.

Where things live (under COORD_ROOT, default /social-network/m4):
    /members/<process-id>       every process that uses coordination
    /election/candidate-<seq>   one per coordinator candidate
    /coordinator                the ready leader's "I'm in charge" note
    /locks/post-write/state     the lock state (Member 3 owns the format)

How the election works:
  1. each coordinator makes an ephemeral sequential candidate znode
  2. smallest number wins
  3. everyone else only watches the one right before them
  4. the winner makes sure the lock state exists (never overwrites it) and then
     posts /coordinator in a transaction that also checks its own candidate

generation = the winner's candidate number, so it only ever goes up.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import KazooException, NoNodeError, NodeExistsError

log = logging.getLogger("zk_coordination")

DEFAULT_HOSTS = "127.0.0.1:2181"
DEFAULT_ROOT = "/social-network/m4"
CANDIDATE_PREFIX = "candidate-"
RESOURCE = "post-write"


class CoordinationUnavailable(Exception):
    """Can't confirm who's in charge right now, so we say no."""


class NotLeaderError(CoordinationUnavailable):
    """This node isn't the leader (or can't prove it anymore)."""


def encode_state(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def decode_state(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


def _default_state_factory() -> Callable[[], dict]:
    from coordination_state import initial_state  # Member 3's schema

    return initial_state


class ZKCoordination:
    def __init__(
        self,
        node_id: str,
        clock: Any,
        *,
        hosts: str,
        root: str,
        advertised_url: Optional[str] = None,
        session_timeout: float = 10.0,
        state_factory: Optional[Callable[[], dict]] = None,
    ) -> None:
        # node_id = ctx.node_id (like "coord-1:<boot-id>"), clock = the
        # MessageContext this process already has. we don't make our own clock.
        if not node_id or "/" in node_id:
            raise ValueError("node_id must be non-empty and contain no '/'")
        root = "/" + root.strip("/")
        if root == "/":
            raise ValueError("root must not be '/'")
        self.node_id = node_id
        self.clock = clock
        self.hosts = hosts
        self.root = root
        self.advertised_url = advertised_url
        self.session_timeout = session_timeout
        self._state_factory = state_factory

        self.members_path = f"{root}/members"
        self.election_path = f"{root}/election"
        self.advert_path = f"{root}/coordinator"
        self.lock_dir = f"{root}/locks/{RESOURCE}"
        self.lock_state_path = f"{self.lock_dir}/state"
        self._member_path = f"{self.members_path}/{node_id}"

        self.client = KazooClient(hosts=hosts, timeout=session_timeout)

        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._closed = False

        # election stuff, always touch it with _lock
        self._candidate_path: Optional[str] = None
        self._candidate_version: Optional[int] = None
        self._candidate_session: Optional[int] = None
        self._generation: Optional[int] = None
        self._attempt: Optional[str] = None
        self._established = False  # only true once we're verified AND advertised
        self._watched: Optional[str] = None

    # ---------------- public ----------------
    @property
    def candidate_path(self) -> Optional[str]:
        with self._lock:
            return self._candidate_path

    @property
    def generation(self) -> Optional[int]:
        with self._lock:
            return self._generation

    def start(self) -> None:
        if self._thread is not None:
            return
        self.client.add_listener(self._on_state)
        self.client.start(timeout=self.session_timeout * 2)
        for path in (self.members_path, self.election_path, self.lock_dir):
            self.client.ensure_path(path)
        self._register_member_blocking()
        if self.advertised_url is not None:
            self._step()  # join the election right away
        self._thread = threading.Thread(
            target=self._run, name=f"zk-election-{self.node_id}", daemon=True
        )
        self._thread.start()
        self._wake.set()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self._lock:
            self._established = False
            cand = self._candidate_path
        self._stop.set()
        self._wake.set()
        t = self._thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5)
        # step down now so the next one doesn't wait for the session to time out
        try:
            if self.client.connected:
                if cand:
                    self._withdraw_advert(cand)
                    self._delete_quiet(cand)
                self._delete_quiet(self._member_path)
        except Exception:  # noqa: BLE001 - not a big deal if this fails
            log.debug("resign failed", exc_info=True)
        try:
            self.client.remove_listener(self._on_state)
            self.client.stop()
        finally:
            self.client.close()

    def leader_info(self) -> Optional[dict]:
        """Gives back the ready leader's info, or None if there isn't one.

        None means we're disconnected, nobody advertised, the advert doesn't
        match the smallest candidate, the lock state is missing, or it's us
        and we can't confirm we're leader right now.
        """
        c = self.client
        if not c.connected:
            return None
        for _ in range(5):  # try a few times in case a node gets deleted mid-read
            try:
                try:
                    c.sync(self.advert_path)
                except KazooException:
                    pass
                try:
                    raw, stat = c.get(self.advert_path)
                except NoNodeError:
                    return None
                adv = decode_state(raw)
                cands = self._candidates()
                if not cands:
                    return None
                seq, name = cands[0]
                first = f"{self.election_path}/{name}"
                if adv.get("candidate_path") != first or adv.get("generation") != seq:
                    return None  # old advert, new leader isn't ready yet
                if c.exists(self.lock_state_path) is None:
                    return None
                with self._lock:
                    mine = first == self._candidate_path
                    est = self._established
                if mine and not est:
                    return None
                adv["session_id"] = stat.owner_session_id
                adv["ready"] = True
                return adv
            except NoNodeError:
                continue
            except KazooException:
                return None
        return None

    def is_leader(self) -> bool:
        """Asks ZooKeeper right now (not just a saved True/False)."""
        try:
            self.leadership_token()
            return True
        except CoordinationUnavailable:
            return False

    # ---- stuff Member 3 and Member 5 call ----
    def leadership_token(self) -> dict:
        """Proves we're leader right now, or raises NotLeaderError."""
        with self._lock:
            path = self._candidate_path
            version = self._candidate_version
            est = self._established
            gen = self._generation
        c = self.client
        if not est or path is None or not c.connected:
            raise NotLeaderError(f"{self.node_id} is not an established leader")
        try:
            stat = c.exists(path)
            if (
                stat is None
                or stat.version != version
                or stat.owner_session_id != c.client_id[0]
            ):
                self._wake.set()
                raise NotLeaderError("candidate znode changed or gone")
            cands = self._candidates()
        except KazooException as exc:
            raise CoordinationUnavailable(f"cannot verify leadership: {exc}") from exc
        if not cands or f"{self.election_path}/{cands[0][1]}" != path:
            self._wake.set()
            raise NotLeaderError("a smaller candidate exists")
        return {
            "node_id": self.node_id,
            "url": self.advertised_url,
            "candidate_path": path,
            "version": version,
            "generation": gen,
            "session_id": stat.owner_session_id,
        }

    def add_leadership_check(self, txn) -> dict:
        """Adds the "am I still leader" check to a transaction."""
        tok = self.leadership_token()
        txn.check(tok["candidate_path"], tok["version"])
        return tok

    def leader_txn(self):
        """Makes a transaction that already has the leadership check in it.

        Add your set_data (with the version) and commit. If any result is an
        Exception, nothing got applied. ConnectionLoss on commit means we don't
        know, so reread the state and don't grant again.
        """
        txn = self.client.transaction()
        self.add_leadership_check(txn)
        return txn

    def read_lock_state(self, *, sync: bool = True) -> Tuple[dict, int]:
        """Returns (state dict, zk version). Raises if we can't read it."""
        c = self.client
        if not c.connected:
            raise CoordinationUnavailable("not connected to ZooKeeper")
        try:
            if sync:
                c.sync(self.lock_state_path)
            raw, stat = c.get(self.lock_state_path)
        except NoNodeError as exc:
            raise CoordinationUnavailable("lock state does not exist") from exc
        except KazooException as exc:
            raise CoordinationUnavailable(f"cannot read lock state: {exc}") from exc
        return decode_state(raw), stat.version

    def members(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for name in self.client.get_children(self.members_path):
            try:
                raw, _ = self.client.get(f"{self.members_path}/{name}")
                out[name] = decode_state(raw)
            except NoNodeError:
                continue
        return out

    # ---------------- clock / logging ----------------
    def _vclock(self):
        return getattr(self.clock, "clock", self.clock)  # ctx.clock, or the clock itself

    def _tick(self) -> Any:
        """One local event on the clock, returns the new vector."""
        try:
            return self._vclock().tick()
        except Exception:  # noqa: BLE001 - clock problem shouldn't kill the election
            log.warning("clock tick failed", exc_info=True)
            return None

    def _snapshot(self) -> Any:
        """Just reads the vector, doesn't tick."""
        try:
            return self._vclock().snapshot()
        except Exception:  # noqa: BLE001
            return None

    def _event(self, event: str, request_id: str, **fields: Any) -> Any:
        """Tick the clock and write it to the log (ctx.local_event)."""
        try:
            if hasattr(self.clock, "local_event") and hasattr(self.clock, "log"):
                return self.clock.local_event(event, request_id, **fields)["vector_clock"]
            vec = self._tick()
            log.info("%s", json.dumps(dict(fields, event=event, node_id=self.node_id,
                                           request_id=request_id, vector_clock=vec),
                                      default=str, sort_keys=True))
            return vec
        except Exception:  # noqa: BLE001
            log.warning("event log failed: %s", event, exc_info=True)
            return None

    def _rid(self) -> str:
        return f"election:{self._attempt or self.node_id}"

    # ---------------- internals ----------------
    def _session_id(self) -> int:
        return self.client.client_id[0]

    def _delete_quiet(self, path: str) -> None:
        try:
            self.client.delete(path)
        except NoNodeError:
            pass

    def _on_state(self, state: str) -> None:
        # this runs on kazoo's thread so don't block in here
        lost = False
        gen = None
        rid = self._rid()
        with self._lock:
            self._watched = None
            if state != KazooState.CONNECTED:  # suspended or lost = not the leader anymore
                lost = self._established
                gen = self._generation
                self._established = False
                if state == KazooState.LOST:
                    self._drop_candidate_locked()
        if lost:
            self._event("LEADERSHIP_LOST", rid, coordinator_generation=gen,
                        reason=f"zk-{state}")
        log.info("%s zk state -> %s", self.node_id, state)
        self._wake.set()

    def _drop_candidate_locked(self) -> None:
        self._candidate_path = None
        self._candidate_version = None
        self._candidate_session = None
        self._generation = None
        self._established = False
        self._watched = None

    def _drop_candidate(self, reason: str) -> None:
        with self._lock:
            lost = self._established
            gen = self._generation
            rid = self._rid()
            self._drop_candidate_locked()
        if lost:
            self._event("LEADERSHIP_LOST", rid, coordinator_generation=gen, reason=reason)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(timeout=1.0)
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                self._step()
            except NoNodeError:
                self._stop.wait(0.2)
                self._wake.set()
            except KazooException as exc:
                log.warning("%s election step: %s", self.node_id, exc)
            except Exception:  # noqa: BLE001
                log.exception("%s election step crashed", self.node_id)

    def _step(self) -> None:
        if not self.client.connected:
            return
        session = self._session_id()
        self._ensure_member(session)
        if self.advertised_url is None:
            return  # discovery only, never a candidate
        with self._lock:
            stale = self._candidate_path is not None and self._candidate_session != session
        if stale:
            self._drop_candidate("session changed")  # new session = rejoin the election
        with self._lock:
            have = self._candidate_path is not None
        if not have:
            self._join_election()
        self._evaluate()

    # -- membership
    def _ensure_member(self, session: int) -> bool:
        c = self.client
        stat = c.exists(self._member_path)
        if stat is not None:
            if stat.owner_session_id != session:
                log.warning("%s: stale member znode from an old session; waiting", self.node_id)
                return False
            return True
        data = {
            "node_id": self.node_id,
            "url": self.advertised_url,
            "role": "coordinator" if self.advertised_url else "client",
            "vector_clock": self._snapshot(),
        }
        try:
            c.create(self._member_path, encode_state(data), ephemeral=True)
        except NodeExistsError:
            return False  # we'll sort it out next step
        return True

    def _register_member_blocking(self) -> None:
        deadline = time.time() + self.session_timeout * 2
        while time.time() < deadline:
            try:
                if self._ensure_member(self._session_id()):
                    return
            except KazooException:
                pass
            time.sleep(0.2)
        log.warning("%s: member registration still pending after start()", self.node_id)

    # -- election
    def _candidates(self) -> List[Tuple[int, str]]:
        out = []
        for n in self.client.get_children(self.election_path):
            if n.startswith(CANDIDATE_PREFIX):
                try:
                    out.append((int(n[len(CANDIDATE_PREFIX):]), n))
                except ValueError:
                    continue
        out.sort()
        return out

    def _join_election(self) -> None:
        attempt = uuid.uuid4().hex
        vc = self._tick()  # logged after the znode is made
        data = {
            "node_id": self.node_id,
            "url": self.advertised_url,
            "attempt_id": attempt,
            "vector_clock": vc,
        }
        payload = encode_state(data)
        prefix = f"{self.election_path}/{CANDIDATE_PREFIX}"
        path = None
        while path is None and not self._stop.is_set():
            try:
                path = self.client.create(prefix, payload, ephemeral=True, sequence=True)
            except KazooException:
                # not sure if the create worked, so look for ours by attempt_id
                # instead of making a second candidate
                path = self._find_candidate(attempt)
                if path is None:
                    time.sleep(0.2)
        if path is None:
            return
        try:
            _, stat = self.client.get(path)
        except NoNodeError:
            return  # already gone, next step rejoins
        gen = int(path[len(prefix):])
        with self._lock:
            self._candidate_path = path
            self._candidate_version = stat.version
            self._candidate_session = stat.owner_session_id
            self._generation = gen
            self._attempt = attempt
            self._established = False
            self._watched = None
        self._event("ELECTION_JOIN", f"election:{attempt}",
                    coordinator_generation=gen, candidate_path=path)

    def _find_candidate(self, attempt: str) -> Optional[str]:
        try:
            if not self.client.connected:
                return None
            for _, name in self._candidates():
                p = f"{self.election_path}/{name}"
                try:
                    raw, _ = self.client.get(p)
                except NoNodeError:
                    continue
                if decode_state(raw).get("attempt_id") == attempt:
                    return p
        except KazooException:
            return None
        return None

    def _evaluate(self) -> None:
        with self._lock:
            path = self._candidate_path
        if path is None:
            return
        names = [n for _, n in self._candidates()]
        me = path.rsplit("/", 1)[1]
        if me not in names:  # our node got deleted, so rejoin
            self._withdraw_advert(path)
            self._drop_candidate("candidate deleted")
            self._wake.set()
            return
        idx = names.index(me)
        if idx == 0:
            self._become_leader(path)
        else:
            with self._lock:
                self._established = False
            self._watch_predecessor(f"{self.election_path}/{names[idx - 1]}")

    def _watch_predecessor(self, pred: str) -> None:
        with self._lock:
            if self._watched == pred:
                return
        stat = self.client.exists(pred, watch=self._on_predecessor_event)
        if stat is None:  # predecessor is already gone, check again
            self._wake.set()
        else:
            with self._lock:
                self._watched = pred

    def _on_predecessor_event(self, event) -> None:
        with self._lock:
            self._watched = None
        self._wake.set()

    def _become_leader(self, path: str) -> None:
        with self._lock:
            if self._established:
                return
            gen = self._generation
            rid = self._rid()
        self._ensure_lock_state()
        vc = self._tick()  # this vector goes in the /coordinator note
        if not self._advertise(path, gen, vc):
            self._wake.set()
            return
        cands = self._candidates()  # make sure we're still first
        if not cands or f"{self.election_path}/{cands[0][1]}" != path:
            self._wake.set()
            return
        with self._lock:
            if self._candidate_path != path or not self.client.connected:
                return
            self._established = True
        self._event("LEADER_READY", rid, coordinator_generation=gen,
                    candidate_path=path)

    def _ensure_lock_state(self) -> None:
        """Makes the lock state if it's missing. Never overwrites it."""
        c = self.client
        if c.exists(self.lock_state_path) is not None:
            return
        factory = self._state_factory or _default_state_factory()
        try:
            c.create(self.lock_state_path, encode_state(factory()), makepath=True)
        except NodeExistsError:
            pass  # someone already made it, fine

    def _advertise(self, path: str, gen: int, vc: Any) -> bool:
        """Posts /coordinator in the same transaction that checks our candidate.
        Safe to call again if the last commit was unclear."""
        c = self.client
        with self._lock:
            version = self._candidate_version
        try:
            raw, stat = c.get(self.advert_path)
        except NoNodeError:
            raw, stat = None, None
        if stat is not None:
            cur = decode_state(raw)
            if stat.owner_session_id == self._session_id() and cur.get("candidate_path") == path:
                return True  # already ours, earlier commit worked
        adv = {
            "node_id": self.node_id,
            "url": self.advertised_url,
            "candidate_path": path,
            "generation": gen,
            "vector_clock": vc,
        }
        txn = c.transaction()
        txn.check(path, version)
        if stat is not None:  # leftover advert from the old leader
            txn.delete(self.advert_path, version=stat.version)
        txn.create(self.advert_path, encode_state(adv), ephemeral=True)
        results = txn.commit()  # if connection drops we just recheck next step
        return not any(isinstance(r, Exception) for r in results)

    def _withdraw_advert(self, candidate_path: str) -> None:
        try:
            raw, stat = self.client.get(self.advert_path)
            if decode_state(raw).get("candidate_path") == candidate_path:
                self.client.delete(self.advert_path, version=stat.version)
        except KazooException:
            pass