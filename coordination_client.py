"""Post's HTTP client for the elected ZooKeeper coordination leader.

ZooKeeper is used only to discover the ready coordinator. Lock decisions are
made by that coordinator's HTTP request--grant--release protocol.
"""

from __future__ import annotations

import os
import time
from urllib.parse import urlparse

import requests

RESOURCE = "post-write"
DEFAULT_HOSTS = "127.0.0.1:2181"
DEFAULT_ROOT = "/social-network/m4"
DEFAULT_TIMEOUT = 2.0
MAX_RETRY_DELAY = 0.5


class CoordinationClientError(RuntimeError):
    """Coordination is unavailable or returned a response outside its contract."""


class CoordinationClient:
    """Discover a ready leader and retry lock calls using stable operation IDs."""

    def __init__(self, message_context, *, hosts=None, root=None, zk=None,
                 session=None, timeout=DEFAULT_TIMEOUT, sleep=time.sleep):
        if message_context is None or not getattr(message_context, "node_id", None):
            raise ValueError("message_context with a process node_id is required")
        self.message_context = message_context
        self.hosts = hosts or os.getenv("ZK_HOSTS", DEFAULT_HOSTS)
        self.root = root or os.getenv("COORD_ROOT", DEFAULT_ROOT)
        self.timeout = float(timeout)
        self.sleep = sleep
        self.session = session or requests.Session()
        if zk is None:
            try:
                from zk_coordination import ZKCoordination
                zk = ZKCoordination(
                    message_context.node_id, message_context, hosts=self.hosts,
                    root=self.root, advertised_url=None)
            except Exception as exc:
                raise CoordinationClientError(
                    "cannot load ZooKeeper discovery client: %s" % exc) from exc
        self.zk = zk
        try:
            self.zk.start()
        except Exception as exc:
            try:
                self.zk.close()
            except Exception:
                pass
            raise CoordinationClientError(
                "cannot start ZooKeeper coordinator discovery: %s" % exc) from exc
        self._closed = False

    def _leader(self):
        if self._closed:
            raise CoordinationClientError("coordination client is closed")
        try:
            leader = self.zk.leader_info()
        except Exception:
            return None
        if not isinstance(leader, dict) or leader.get("ready") is not True:
            return None
        url = leader.get("url")
        parsed = urlparse(url) if isinstance(url, str) else None
        if (parsed is None or parsed.scheme not in ("http", "https") or
                not parsed.hostname or parsed.path not in ("", "/") or
                parsed.query or parsed.fragment):
            return None
        return leader

    def _pause(self, attempt):
        self.sleep(min(0.05 * (2 ** min(attempt, 4)), MAX_RETRY_DELAY))

    def _post(self, path, body, request_id, *, wait_responses=(), max_attempts=None):
        """Retry transport failures against freshly discovered leaders.

        A retry is a new clock send but retains the same request ID. It never
        retries a Post mutation; these calls only acquire or release a grant.
        """
        attempt = 0
        waiting_logged = False
        while not self._closed:
            if max_attempts is not None and attempt >= max_attempts:
                raise CoordinationClientError(
                    "coordination request did not complete after %d attempts" % attempt)
            leader = self._leader()
            if leader is None:
                self._pause(attempt)
                attempt += 1
                continue
            rid = request_id
            headers = self.message_context.send_headers(rid)
            url = leader["url"].rstrip("/") + path
            try:
                response = self.session.post(url, json=body, headers=headers,
                                             timeout=self.timeout)
            except requests.RequestException:
                # The request may have committed. Same request ID makes acquire
                # idempotent; release is defined as safe to repeat.
                self._pause(attempt)
                attempt += 1
                continue
            try:
                response_request_id = self.message_context.receive_headers(response.headers)
            except Exception as exc:
                raise CoordinationClientError(
                    "coordinator response metadata is invalid: %s" % exc) from exc
            if response_request_id != rid:
                raise CoordinationClientError(
                    "coordinator reply correlation ID did not match request")
            try:
                result = response.json()
            except ValueError as exc:
                self._pause(attempt)
                attempt += 1
                continue

            if response.status_code in wait_responses:
                if not waiting_logged:
                    self.message_context.log(
                        "LOCK_WAIT", rid, resource=body.get("resource", RESOURCE),
                        coordinator_generation=leader.get("generation"))
                    waiting_logged = True
                self._pause(attempt)
                attempt += 1
                continue
            if response.status_code == 503:
                # Coordinator may have changed or lost ZooKeeper authority.
                self._pause(attempt)
                attempt += 1
                continue
            if not isinstance(result, dict):
                raise CoordinationClientError("coordinator reply must be a JSON object")
            return response, result, leader
        raise CoordinationClientError("coordination client closed while waiting")

    def acquire(self, resource, request_id):
        if resource != RESOURCE:
            raise ValueError("only the global %r resource is supported" % RESOURCE)
        self.message_context.log("LOCK_REQUEST", request_id, resource=resource)
        response, result, leader = self._post(
            "/locks/acquire", {"resource": resource, "request_id": request_id},
            request_id, wait_responses=(202,))
        grant = result.get("grant")
        if (response.status_code != 200 or result.get("status") != "granted" or
                not isinstance(grant, dict) or grant.get("resource") != resource or
                grant.get("request_id") != request_id or
                grant.get("owner_id") != self.message_context.node_id or
                type(grant.get("token")) is not int or grant["token"] < 1):
            raise CoordinationClientError(
                "coordinator did not return a valid grant: HTTP %s %s" %
                (response.status_code, result))
        self.message_context.log(
            "LOCK_GRANTED", request_id, resource=resource,
            grant_token=grant["token"],
            coordinator_generation=leader.get("generation"))
        return dict(grant)

    def release(self, grant):
        if not isinstance(grant, dict) or grant.get("resource") != RESOURCE:
            raise ValueError("a complete post-write grant is required")
        request_id = grant.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("grant has no request_id")
        response, result, leader = self._post(
            "/locks/release", {"grant": grant}, request_id, max_attempts=8)
        if response.status_code != 200 or result.get("status") != "released":
            raise CoordinationClientError(
                "coordinator rejected grant release: HTTP %s %s" %
                (response.status_code, result))
        self.message_context.log(
            "LOCK_RELEASE", request_id, resource=grant["resource"],
            grant_token=grant.get("token"),
            coordinator_generation=leader.get("generation"))

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.zk.close()
