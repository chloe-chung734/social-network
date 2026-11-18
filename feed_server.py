"""
Feed service for CECS 327 Milestones 3 and 4: P2P and coordination.

Feed asks Profile who the user follows, then asks Post for their posts.
Post will use the group's four Chord storage peers to find those posts.
Joining, leaving, and peer lookups are handled by Chord, not by Feed.

Feed returns the posts newest first, without duplicate post IDs.
It allows 10 seconds per Post request for peer lookups. If a service is
unavailable or times out, Feed returns an error instead of a partial feed.
Post must report missing data as an error; Feed cannot spot silent omissions.

Milestone 4 uses Member 1's MessageContext for HTTP and group messages.
Vector clocks trace causality; post creation timestamps still order the feed.
"""

import argparse
import json
import logging
import os
import socketserver
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, g, jsonify, request
from urllib3.exceptions import ReadTimeoutError
from werkzeug.serving import make_server


POST_TIMEOUT = 10  # Give Post time to look up posts through the peers.
_process_context = None


def process_context(node_name="feed"):
    """Reuse one clock across HTTP, subscription, and the TCP receiver."""
    global _process_context
    if _process_context is None:
        try:
            from message_protocol import MessageContext
        except ModuleNotFoundError as error:
            if error.name != "message_protocol":
                raise
            if os.getenv("COORDINATION_ENABLED", "0") == "1":
                raise RuntimeError("Milestone 4 requires Member 1's message_protocol.py") from error
            return None  # Explicit Milestone 3 compatibility; no fabricated clock.
        _process_context = MessageContext(os.getenv("NODE_NAME", node_name))
    return _process_context


def make_logger(name, log_dir="logs"):
    """Each service writes UTC timestamps to both a file and the terminal."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)sZ [%(name)s] %(message)s", "%Y-%m-%dT%H:%M:%S"
        )
        formatter.converter = time.gmtime
        for handler in (logging.StreamHandler(),
                        logging.FileHandler(Path(log_dir) / f"{name}.log")):
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    return logger


def marshal(message, logger, label):
    """Python dictionary -> JSON text -> UTF-8 bytes (visible in demo logs)."""
    logger.info("%s BEFORE_MARSHAL %r", label, message)
    wire = json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8")
    logger.info("%s AFTER_MARSHAL %r", label, wire)
    return wire


def unmarshal(wire, logger, label):
    """UTF-8 bytes -> JSON text -> Python data."""
    logger.info("%s BEFORE_UNMARSHAL %r", label, wire)
    message = json.loads(wire.decode("utf-8"))
    logger.info("%s AFTER_UNMARSHAL %r", label, message)
    return message


class ServiceError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


def request_json(method, url, logger, request_id, body=None, timeout=2, not_found_error=None,
                 message_context=None):
    """Call another service. Explicit serialization makes the wire format clear."""
    headers = {"Content-Type": "application/json", "X-Request-ID": request_id}
    if message_context is not None:
        headers = message_context.send_headers(request_id, extra=headers)
        message_context.log("HTTP_SEND", request_id, method=method, url=url)
    wire = None if body is None else marshal(body, logger, request_id)
    logger.info("SEND %s %s request_id=%s", method, url, request_id)
    try:
        response = requests.request(method, url, data=wire, headers=headers,
                                    timeout=timeout)
        if message_context is not None:
            message_context.receive_headers(response.headers)
            message_context.log("HTTP_RECV", request_id, status=response.status_code, url=url)
        logger.info("RECV status=%s request_id=%s", response.status_code, request_id)
        if response.status_code == 503:
            raise ServiceError(f"Dependency temporarily unavailable: {url}", 503)
        if response.status_code == 504:
            raise ServiceError(f"Dependency timed out: {url}", 504)
        if response.status_code == 404 and not_found_error is not None:
            error_body = unmarshal(response.content, logger, request_id)

            if (isinstance(error_body, dict)
                    and error_body.get("error") == not_found_error):
                raise ServiceError(not_found_error, 404)

        response.raise_for_status()
        result = unmarshal(response.content, logger, request_id)
    except requests.Timeout as error:
        raise ServiceError(f"Dependency timed out: {url}", 504) from error
    except requests.ConnectionError as error:
        # requests wraps a timeout while reading the body as ConnectionError.
        if error.args and isinstance(error.args[0], ReadTimeoutError):
            raise ServiceError(f"Dependency timed out: {url}", 504) from error
        raise ServiceError(f"Dependency unavailable: {url}", 503) from error
    except (requests.RequestException, ValueError, TypeError) as error:
        raise ServiceError(f"Dependency failed or returned invalid JSON: {url}") from error
    if not isinstance(result, dict):
        raise ServiceError(f"Expected a JSON object from {url}")
    return result


def valid_id(value):
    # This prototype agrees on positive integer user/post IDs at HTTP boundaries.
    return type(value) is int and value > 0


def post_time(post):
    """Parse timestamps before sorting; offsets may differ between messages."""
    if not isinstance(post, dict):
        raise ValueError("post must be an object")
    if not valid_id(post.get("id")) or not valid_id(post.get("author")):
        raise ValueError("post id and author must be positive integers")
    for field in ("timestamp", "content", "caption"):
        if not isinstance(post.get(field), str):
            raise ValueError(f"post {field} must be a string")
    timestamp = datetime.fromisoformat(post["timestamp"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("post timestamp must include its UTC offset")
    return timestamp


class FeedServer:
    def __init__(self, profile_url, post_url, logger, timeout=2, demo_following=None,
                 message_context=None):
        self.profile_url = profile_url.rstrip("/")
        self.post_url = post_url.rstrip("/")
        self.logger = logger
        self.timeout = timeout
        self.demo_following = demo_following
        self.message_context = message_context
        # Event observations are separate from authoritative feed retrieval.
        self.received_posts = {}
        self.seen_events = {}
        self.lock = threading.Lock()

    def get_following(self, user_id, request_id):
        """Use Profile normally; use a supplied fixture only when explicitly enabled."""
        if self.demo_following is not None:
            if user_id not in self.demo_following:
                raise ServiceError("User is not included in --demo-following", 404)
            self.logger.info("DEMO_FOLLOWING user_id=%s authors=%s", user_id,
                             self.demo_following[user_id])
            return self.demo_following[user_id]
        profile = request_json(
            "GET", f"{self.profile_url}/profiles/{user_id}/following",
            self.logger, request_id, timeout=self.timeout,
            message_context=self.message_context
        )
        following = profile.get("following")
        if (not valid_id(profile.get("user_id")) or profile["user_id"] != user_id
                or not isinstance(following, list)
                or not all(valid_id(author) for author in following)):
            raise ServiceError("Profile must return user_id and a list of integer following IDs")
        return following

    def get_feed(self, user_id, request_id):
        following = self.get_following(user_id, request_id)

        posts_by_id = {}
        for author in dict.fromkeys(following):
            # Post will handle the Chord lookup.
            result = request_json(
                "POST", f"{self.post_url}/posts/retrieve", self.logger,
                request_id, {"author": author}, timeout=POST_TIMEOUT,
                message_context=self.message_context
            )
            if result.get("success") is not True or not isinstance(result.get("posts"), list):
                raise ServiceError("Post must return success=true and a posts list")
            for post in result["posts"]:
                try:
                    post_time(post)
                    if post["author"] != author:
                        raise ValueError("Post returned a different author's post")
                except (ValueError, TypeError) as error:
                    raise ServiceError(f"Invalid Post response: {error}") from error
                posts_by_id[post["id"]] = post

        posts = sorted(posts_by_id.values(), key=post_time, reverse=True)
        self.logger.info("FEED_READY user_id=%s posts=%s request_id=%s",
                         user_id, len(posts), request_id)
        return posts

    def receive_event(self, event):
        """The broker calls us after matching a post.created subscription."""
        if not isinstance(event, dict) or event.get("type") != "post.created":
            raise ValueError("expected a post.created event object")
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id must be a nonempty string")
        post = event.get("post")
        post_time(post)
        # The lock keeps duplicate checking and insertion one atomic operation.
        with self.lock:
            if event_id in self.seen_events:
                if self.seen_events[event_id] != event:
                    raise ValueError("event_id was reused with different data")
                return True
            self.seen_events[event_id] = event
            self.received_posts[post["id"]] = post
        self.logger.info("EVENT_APPLIED event_id=%s post_id=%s", event_id, post["id"])
        return False


def create_app(profile_url="http://127.0.0.1:5001",
               post_url="http://127.0.0.1:5002", log_dir="logs", timeout=2,
               demo_following=None, node_name="feed", message_context=None):
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
    logger = make_logger(node_name, log_dir)
    context = message_context if message_context is not None else process_context(node_name)
    if context is None:
        logger.info("CLOCK_DISABLED: message_protocol unavailable; Milestone 3 mode")
    feed = FeedServer(profile_url, post_url, logger, timeout, demo_following, context)
    app.config["FEED_SERVICE"] = feed
    app.config["MESSAGE_CONTEXT"] = context

    @app.before_request
    def log_request():
        g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        if context is not None:
            try:
                g.request_id = context.receive_headers(request.headers) or g.request_id
                context.log("HTTP_RECV", g.request_id, method=request.method, path=request.path)
            except (ValueError, TypeError) as error:
                # A malformed incoming request ID cannot be reused by the strict
                # response-header helper. Give this rejected request a safe ID.
                g.request_id = str(uuid.uuid4())
                return jsonify({"success": False, "error": f"Invalid clock metadata: {error}"}), 400
        logger.info("RECV %s %s request_id=%s", request.method, request.path, g.request_id)

    @app.after_request
    def log_reply(response):
        response.headers["X-Request-ID"] = g.request_id
        if context is not None:
            response.headers.update(context.send_headers(g.request_id))
            context.log("HTTP_SEND", g.request_id, status=response.status_code, path=request.path)
        logger.info("SEND status=%s request_id=%s body=%s",
                    response.status_code, g.request_id, response.get_data(as_text=True))
        return response

    @app.get("/health")
    def health():
        return jsonify({"service": "feed", "status": "ok"})

    @app.get("/feed/<int:user_id>")
    def get_feed(user_id):
        if not valid_id(user_id):
            return jsonify({"success": False, "error": "user_id must be positive"}), 400
        try:
            posts = feed.get_feed(user_id, g.request_id)
            reply = {"success": True, "user_id": user_id, "posts": posts,
                     "following_source": "demo" if demo_following is not None else "profile"}
            # Return the exact serialized bytes shown in the marshalling log.
            return app.response_class(marshal(reply, logger, g.request_id),
                                      mimetype="application/json")
        except ServiceError as error:
            logger.warning("FEED_FAILED user_id=%s request_id=%s status=%s reason=%s",
                           user_id, g.request_id, error.status, error)
            return jsonify({"success": False, "error": str(error)}), error.status

    @app.post("/events")
    def receive_event():
        try:
            event = unmarshal(request.get_data(), logger, g.request_id)
            # HTTP headers are the transport clock. Do not merge an embedded
            # publisher clock again, or compare delivery metadata for deduplication.
            if isinstance(event, dict):
                event = {key: value for key, value in event.items() if key != "_meta"}
            duplicate = feed.receive_event(event)
            return jsonify({"success": True, "event_id": event["event_id"],
                            "duplicate": duplicate})
        except (ValueError, TypeError) as error:
            return jsonify({"success": False, "error": str(error)}), 400

    return app


def start_group_receiver(host, port, node_name="feed", log_dir="logs", message_context=None):
    """One JSON line per TCP connection. Coordinator sends to each group member."""
    logger = make_logger(node_name, log_dir)
    context = message_context if message_context is not None else process_context(node_name)

    class GroupHandler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(3)
            request_id = str(uuid.uuid4())
            try:
                line = self.rfile.readline(65537)
                if len(line) > 65536 or not line.endswith(b"\n"):
                    raise ValueError("expected a JSON line of at most 65536 bytes")
                message = unmarshal(line, logger, "GROUP")
                if context is not None:
                    message, request_id = context.receive_payload(message)
                    context.log("GROUP_RECV", request_id)
                if (not isinstance(message, dict)
                        or message.get("group") != "service_nodes"
                        or message.get("type") != "demo_status_request"
                        or not isinstance(message.get("message_id"), str)
                        or not message["message_id"].strip()):
                    raise ValueError("invalid group, type, or message_id")
                logger.info("GROUP_RECEIVED node=%s message_id=%s", node_name,
                            message["message_id"])
                reply = {"success": True, "node": node_name,
                         "message_id": message["message_id"], "status": "ready"}
            except (ValueError, TypeError, OSError) as error:
                reply = {"success": False, "node": node_name, "error": str(error)}
            try:
                if context is not None:
                    reply = context.send_payload(reply, request_id)
                    context.log("GROUP_SEND", request_id)
                self.wfile.write(marshal(reply, logger, "GROUP_REPLY") + b"\n")
            except OSError:
                logger.warning("Group sender disconnected before the reply")

    class GroupTCPServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = GroupTCPServer((host, port), GroupHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("GROUP_LISTEN %s:%s", *server.server_address)
    return server


def subscribe(broker_url, callback_url, logger, message_context=None):
    """Subscribe through the existing broker API."""
    result = request_json("POST", broker_url.rstrip("/") + "/subscribe", logger,
                          str(uuid.uuid4()), {"topic": "post.created", "url": callback_url},
                          message_context=message_context)
    if result.get("success") is not True:
        raise ServiceError("Broker did not confirm subscription")
    logger.info("SUBSCRIBED topic=post.created callback=%s", callback_url)
    return result


def parse_demo_following(value):
    """Parse a command-line fixture such as 2:7,9 or 5: (empty following list)."""
    try:
        user, authors = value.split(":", 1)
        user = int(user)
        authors = [int(author) for author in authors.split(",")] if authors else []
        if not valid_id(user) or not all(valid_id(author) for author in authors):
            raise ValueError()
        return user, authors
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use USER:AUTHOR,AUTHOR with positive IDs, e.g. 2:7,9") from error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Feed service for Milestones 3 and 4")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5003)
    parser.add_argument("--group-port", type=int, default=6003)
    parser.add_argument("--node-name", default="feed", choices=["feed", "feed-2", "feed-3"])
    parser.add_argument("--demo-following", action="append", type=parse_demo_following,
                        help="TEST ONLY: USER:AUTHOR,AUTHOR, e.g. 2:7,9; repeat for other users")
    parser.add_argument("--profile-url", default="http://127.0.0.1:5001")
    parser.add_argument("--post-url", default="http://127.0.0.1:5002")
    parser.add_argument("--broker-url", help="Optional broker supporting POST /subscribe")
    parser.add_argument("--callback-url", help="URL the broker uses to reach this node's /events")
    args = parser.parse_args()
    demo_following = dict(args.demo_following) if args.demo_following is not None else None
    logger = make_logger(args.node_name)
    if demo_following is not None:
        logger.warning("DEMO MODE: following lists come from command-line test data")
    if not args.broker_url:
        logger.info("No broker configured; event callback exists but is not subscribed")
    callback_url = args.callback_url or f"http://127.0.0.1:{args.port}/events"
    app = create_app(args.profile_url, args.post_url, demo_following=demo_following,
                     node_name=args.node_name)
    context = app.config["MESSAGE_CONTEXT"]
    group_server = start_group_receiver(args.host, args.group_port, args.node_name,
                                        message_context=context)
    http_server = None
    try:
        # Start HTTP BEFORE subscription, so a broker can deliver immediately.
        # Werkzeug supplies Flask's local HTTP server; no debug/reloader is used.
        http_server = make_server(args.host, args.port, app, threaded=True)
        http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
        http_thread.start()
        logger.info("HTTP_LISTEN %s:%s", args.host, http_server.server_port)
        if args.broker_url:
            subscribe(args.broker_url, callback_url, logger, message_context=context)
        http_thread.join()
    except KeyboardInterrupt:
        logger.info("Stopping Feed service")
    except ServiceError as error:
        logger.error("Startup failed: %s", error)
        raise SystemExit(1)
    finally:
        if http_server is not None:
            http_server.shutdown()
            http_server.server_close()
        group_server.shutdown()
        group_server.server_close()
