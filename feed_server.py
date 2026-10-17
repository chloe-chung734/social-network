"""
Feed service for CECS 327 Milestone 2.
Requires Python 3.10+, Flask, and requests.

REQUEST-REPLY
The client requests GET /feed/<user_id> on HTTP port 5003.
Feed gets the user's following list from the Profile service on port 5001
using GET /profiles/<user_id>/following. It then requests each followed
author's posts from the Post service on port 8001 using POST /posts/retrieve.
Feed validates the posts, removes duplicate post IDs, and returns them
in order from newest to oldest.

PUBLISH-SUBSCRIBE
When started with --broker-url, Feed subscribes to the post.created topic
through the broker's /subscribe endpoint. The broker forwards matching
events to Feed's POST /events endpoint.

Feed validates incoming events, records them in memory, and recognizes
duplicate deliveries using event_id. Displayed feeds are built from fresh
Post service requests; the stored events are separate from feed retrieval.

GROUP COMMUNICATION
Feed also runs a TCP group receiver on port 6003 by default.
group_sender.py sends the same status request and message ID to multiple
Feed nodes in the service_nodes group.

Each node receives newline-terminated JSON, validates it, logs its receipt,
and returns an acknowledgement containing its node name, the original
message_id, and status "ready". This confirms the group receiver responded.

SERIALIZATION AND LOGGING
Messages are marshalled from Python dictionaries into UTF-8 JSON bytes
and unmarshalled back into Python data when received.
UTC timestamped logs show requests, replies, events, group messages,
and data before and after marshalling and unmarshalling.
Logs appear in the terminal and logs/<node-name>.log.

INSTALL AND START
Install dependencies:
    python -m pip install flask requests

Start Profile and Post in their own terminals. For pub-sub, also start
the broker before starting Feed with:
    python feed_server.py --broker-url http://127.0.0.1:5005

Request a feed from another terminal:
    python client.py feed 2

GROUP DEMO
With the first Feed node running, start a second node in another terminal:
    python feed_server.py --port 5006 --group-port 6006 --node-name feed-2

Then run the sender in another terminal:
    python group_sender.py --message-id group-demo-1

Compare the matching message IDs and acknowledgements in both node logs
and logs/group_sender.log.

IMPLEMENTATION NOTES
Event records and duplicate tracking are lost when Feed restarts.
Restart Feed to subscribe again after a broker restart.
The --demo-following option is available for testing; normal operation
gets following lists from Profile.
The /health endpoint checks only the Feed process.
Group delivery has no automatic retries or all-or-nothing guarantee.
"""

import argparse
import json
import logging
import socketserver
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, g, jsonify, request
from werkzeug.serving import make_server


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


def request_json(method, url, logger, request_id, body=None, timeout=2, not_found_error=None):
    """Call another service. Explicit serialization makes the wire format clear."""
    headers = {"Content-Type": "application/json", "X-Request-ID": request_id}
    wire = None if body is None else marshal(body, logger, request_id)
    logger.info("SEND %s %s request_id=%s", method, url, request_id)
    try:
        response = requests.request(method, url, data=wire, headers=headers,
                                    timeout=timeout)
        logger.info("RECV status=%s request_id=%s", response.status_code, request_id)
        if response.status_code == 404 and not_found_error is not None:
            error_body = unmarshal(response.content, logger, request_id)

            if (isinstance(error_body, dict)
                    and error_body.get("error") == not_found_error):
                raise ServiceError(not_found_error, 404)

        response.raise_for_status()
        result = unmarshal(response.content, logger, request_id)
    except requests.Timeout as error:
        raise ServiceError(f"Dependency timed out: {url}", 504) from error
    except (requests.RequestException, ValueError) as error:
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
    def __init__(self, profile_url, post_url, logger, timeout=2, demo_following=None):
        self.profile_url = profile_url.rstrip("/")
        self.post_url = post_url.rstrip("/")
        self.logger = logger
        self.timeout = timeout
        self.demo_following = demo_following
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
        # PROPOSED Profile endpoint: your teammate still needs to provide it.
        profile = request_json(
            "GET", f"{self.profile_url}/profiles/{user_id}/following",
            self.logger, request_id, timeout=self.timeout
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
            # EXISTING Post endpoint and payload from your team's repository.
            result = request_json(
                "POST", f"{self.post_url}/posts/retrieve", self.logger,
                request_id, {"author": author}, timeout=self.timeout
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

        return sorted(posts_by_id.values(), key=post_time, reverse=True)

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
               post_url="http://127.0.0.1:8001", log_dir="logs", timeout=2,
               demo_following=None, node_name="feed"):
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
    logger = make_logger(node_name, log_dir)
    feed = FeedServer(profile_url, post_url, logger, timeout, demo_following)
    app.config["FEED_SERVICE"] = feed

    @app.before_request
    def log_request():
        g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        logger.info("RECV %s %s request_id=%s", request.method, request.path, g.request_id)

    @app.after_request
    def log_reply(response):
        response.headers["X-Request-ID"] = g.request_id
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
            return jsonify({"success": False, "error": str(error)}), error.status

    @app.post("/events")
    def receive_event():
        try:
            event = unmarshal(request.get_data(), logger, g.request_id)
            duplicate = feed.receive_event(event)
            return jsonify({"success": True, "event_id": event["event_id"],
                            "duplicate": duplicate})
        except (ValueError, TypeError) as error:
            return jsonify({"success": False, "error": str(error)}), 400

    return app


def start_group_receiver(host, port, node_name="feed", log_dir="logs"):
    """One JSON line per TCP connection. Coordinator sends to each group member."""
    logger = make_logger(node_name, log_dir)

    class GroupHandler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(3)
            try:
                line = self.rfile.readline(65537)
                if len(line) > 65536 or not line.endswith(b"\n"):
                    raise ValueError("expected a JSON line of at most 65536 bytes")
                message = unmarshal(line, logger, "GROUP")
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
            except (ValueError, OSError) as error:
                reply = {"success": False, "node": node_name, "error": str(error)}
            try:
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


def subscribe(broker_url, callback_url, logger):
    """PROPOSED broker API. Confirm this contract with the broker's owner."""
    result = request_json("POST", broker_url.rstrip("/") + "/subscribe", logger,
                          str(uuid.uuid4()), {"topic": "post.created", "url": callback_url})
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
    parser = argparse.ArgumentParser(description="Feed service for Milestone 2")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5003)
    parser.add_argument("--group-port", type=int, default=6003)
    parser.add_argument("--node-name", default="feed", choices=["feed", "feed-2", "feed-3"])
    parser.add_argument("--demo-following", action="append", type=parse_demo_following,
                        help="TEST ONLY: USER:AUTHOR,AUTHOR, e.g. 2:7,9; repeat for other users")
    parser.add_argument("--profile-url", default="http://127.0.0.1:5001")
    parser.add_argument("--post-url", default="http://127.0.0.1:8001")
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
    group_server = start_group_receiver(args.host, args.group_port, args.node_name)
    http_server = None
    try:
        # Start HTTP BEFORE subscription, so a broker can deliver immediately.
        # Werkzeug supplies Flask's local HTTP server; no debug/reloader is used.
        http_server = make_server(args.host, args.port, app, threaded=True)
        http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
        http_thread.start()
        logger.info("HTTP_LISTEN %s:%s", args.host, http_server.server_port)
        if args.broker_url:
            subscribe(args.broker_url, callback_url, logger)
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
