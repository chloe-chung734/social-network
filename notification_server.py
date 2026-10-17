"""Notification service for the CECS 327 distributed social network prototype.

WHAT THIS SERVER DOES
    Stores notifications for users ("user 7 made a new post", "user 2 liked
    your post") and returns them when the client asks.

HOW NOTIFICATIONS ARRIVE (two ways)
    1. PUB-SUB (main path): at startup this server subscribes to the broker.
       When Post publishes an event, the broker delivers it to POST /events.
           post.created   -> notify every FOLLOWER of the author
                             (asks Profile: GET /profiles/<author>/followers)
           post.liked     -> notify the post's author   (needs Post to publish it)
           post.commented -> notify the post's author   (needs Post to publish it)
    2. DIRECT: any service can still POST /notifications with a ready-made
       notification. Handy for testing with curl.

ENDPOINTS
    POST /events                    broker delivers an event here
    POST /notifications             {"to_user_id": 1, "type": "like", "from_user_id": 2, "post_id": 1}
    GET  /notifications/<user_id>   -> {"success": true, "user_id": 1, "notifications": [...]}
    GET  /health                    process check only

INSTALL AND START (start the broker FIRST, we subscribe once at startup)
    python -m pip install flask requests
    python broker_server.py
    python notification_server.py --broker-url http://127.0.0.1:5005

LIMITS
    Notifications live in memory and are lost on restart. If the broker
    restarts, restart this server so it subscribes again.
"""

import argparse
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, g, jsonify, request
from werkzeug.serving import make_server

# Topics we subscribe to on the broker. The broker forwards an event to us
# only if its "type" exactly matches one of these.
TOPICS = ["post.created", "post.liked", "post.commented"]

# Notification types we store (also what POST /notifications accepts).
VALID_TYPES = {"new_post", "like", "comment", "follow"}


# ---------------------------------------------------------------- logging
def make_logger(name="notification", log_dir="logs"):
    """UTC timestamps to the terminal and logs/notification.log (team style)."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)sZ [%(name)s] %(message)s", "%Y-%m-%dT%H:%M:%S"
        )
        formatter.converter = time.gmtime          # log in UTC, not local time
        for handler in (logging.StreamHandler(),
                        logging.FileHandler(Path(log_dir) / f"{name}.log")):
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    return logger


log = make_logger()

# Flask prints its own line per request; we log our own RECV/SEND instead.
logging.getLogger("werkzeug").setLevel(logging.WARNING)


def marshal(message, label):
    """Python dict -> JSON bytes, logged BEFORE and AFTER (required by the course)."""
    log.info("%s BEFORE_MARSHAL %r", label, message)
    wire = json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8")
    log.info("%s AFTER_MARSHAL %r", label, wire)
    return wire


def unmarshal(wire, label):
    """JSON bytes -> Python dict, logged BEFORE and AFTER."""
    log.info("%s BEFORE_UNMARSHAL %r", label, wire)
    message = json.loads(wire.decode("utf-8"))
    log.info("%s AFTER_UNMARSHAL %r", label, message)
    return message


def valid_id(value):
    """IDs are positive ints. type() is int rejects True/False and "7"."""
    return type(value) is int and value > 0


class DependencyError(Exception):
    """Another service (Profile) failed. We answer 502 so the broker retries."""


# ---------------------------------------------------------------- storage
class NotificationStore:
    """All notification data plus the lock that protects it.

    Flask runs requests on several threads at once. Without the lock, two
    requests could read _next_id at the same moment and reuse an ID, or both
    pass the duplicate check before either records the event.
    """

    def __init__(self):
        self.by_user = {}          # user_id -> list of notifications (oldest first)
        self.seen_events = {}      # event_id -> notifications it created (dedup)
        self.seen_requests = {}    # X-Request-ID -> response (dedup for direct POSTs)
        self.next_id = 1
        self.lock = threading.Lock()

    def _add(self, to_user_id, ntype, from_user_id, post_id):
        """Create one notification. Caller must already hold self.lock."""
        note = {
            "id": self.next_id,
            "to_user_id": to_user_id,
            "type": ntype,
            "from_user_id": from_user_id,
            "post_id": post_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.next_id += 1
        # setdefault creates the user's empty list the first time
        self.by_user.setdefault(to_user_id, []).append(note)
        return note

    def add_from_event(self, event_id, recipients, ntype, from_user_id, post_id):
        """Store one notification per recipient, at most once per event_id.

        Returns (notifications, duplicate). The broker retries failed
        deliveries with the SAME event_id, so a repeat must not add more.
        """
        with self.lock:
            if event_id in self.seen_events:
                return self.seen_events[event_id], True
            notes = [self._add(r, ntype, from_user_id, post_id)
                     for r in recipients
                     if r != from_user_id]          # never notify yourself
            self.seen_events[event_id] = notes
            return notes, False

    def add_direct(self, request_id, to_user_id, ntype, from_user_id, post_id):
        """Store one notification from POST /notifications, deduped by request ID."""
        with self.lock:
            if request_id and request_id in self.seen_requests:
                return self.seen_requests[request_id], True
            note = self._add(to_user_id, ntype, from_user_id, post_id)
            if request_id:
                self.seen_requests[request_id] = note
            return note, False

    def for_user(self, user_id):
        """A user's notifications, newest first (empty list if none)."""
        with self.lock:
            return list(reversed(self.by_user.get(user_id, [])))


store = NotificationStore()
PROFILE_URL = "http://127.0.0.1:5001"   # overwritten by --profile-url


# -------------------------------------------------------- event handling
def get_followers(author_id, request_id):
    """Ask Profile who follows this author: GET /profiles/<id>/followers."""
    url = f"{PROFILE_URL}/profiles/{author_id}/followers"
    log.info("SEND GET %s request_id=%s", url, request_id)
    try:
        response = requests.get(url, headers={"X-Request-ID": request_id}, timeout=2)
        log.info("RECV status=%s request_id=%s", response.status_code, request_id)
        response.raise_for_status()
        body = unmarshal(response.content, request_id)
    except (requests.RequestException, ValueError) as error:
        raise DependencyError(f"Profile followers lookup failed: {error}") from error

    followers = body.get("followers") if isinstance(body, dict) else None
    if not isinstance(followers, list) or not all(valid_id(f) for f in followers):
        raise DependencyError("Profile must return {'followers': [int, ...]}")
    return followers


def handle_event(event, request_id):
    """Turn one broker event into notifications. Returns (notes, duplicate).

    Raises ValueError for a bad event (we answer 400, broker won't retry)
    or DependencyError if Profile is down (we answer 502, broker retries).
    """
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    event_id, topic = event.get("event_id"), event.get("type")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("event_id must be a nonempty string")

    # Already handled? Answer without calling Profile again.
    with store.lock:
        if event_id in store.seen_events:
            return store.seen_events[event_id], True

    if topic == "post.created":
        # {"event_id", "type", "post": {"id": 1, "author": 7, ...}}
        post = event.get("post")
        if not isinstance(post, dict) or not valid_id(post.get("id")) \
                or not valid_id(post.get("author")):
            raise ValueError("post.created needs post.id and post.author (ints)")
        followers = get_followers(post["author"], request_id)
        return store.add_from_event(event_id, followers, "new_post",
                                    post["author"], post["id"])

    if topic in ("post.liked", "post.commented"):
        # {"event_id", "type", "post_id": 1, "post_author": 7, "user_id": 2}
        post_id, author, actor = (event.get("post_id"), event.get("post_author"),
                                  event.get("user_id"))
        if not all(valid_id(v) for v in (post_id, author, actor)):
            raise ValueError(f"{topic} needs post_id, post_author, user_id (ints)")
        ntype = "like" if topic == "post.liked" else "comment"
        return store.add_from_event(event_id, [author], ntype, actor, post_id)

    raise ValueError(f"unsupported event type: {topic!r}")


# ------------------------------------------------------------- HTTP layer
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024


@app.before_request
def log_request():
    """Keep the caller's X-Request-ID (or make one) so logs can be traced."""
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    log.info("RECV %s %s request_id=%s", request.method, request.path, g.request_id)


@app.after_request
def log_reply(response):
    response.headers["X-Request-ID"] = g.request_id
    log.info("SEND status=%s request_id=%s body=%s",
             response.status_code, g.request_id, response.get_data(as_text=True))
    return response


def json_reply(body, status=200):
    """Send the exact bytes shown in the AFTER_MARSHAL log line."""
    return app.response_class(marshal(body, g.request_id), status=status,
                              mimetype="application/json")


def error(message, status):
    return jsonify({"success": False, "error": message}), status


@app.get("/health")
def health():
    return jsonify({"service": "notification", "status": "ok"})


@app.post("/events")
def receive_event():
    """The broker delivers subscribed events here.

    The broker counts a delivery as acknowledged ONLY on HTTP 2xx with
    {"success": true}. 5xx tells it to retry; 4xx means "don't bother".
    """
    try:
        event = unmarshal(request.get_data(), g.request_id)
        notes, duplicate = handle_event(event, g.request_id)
    except DependencyError as exc:
        return error(str(exc), 502)
    except (ValueError, TypeError) as exc:      # includes bad JSON
        return error(str(exc), 400)

    log.info("EVENT_APPLIED event_id=%s created=%d duplicate=%s",
             event["event_id"], len(notes), duplicate)
    return json_reply({"success": True, "event_id": event["event_id"],
                       "duplicate": duplicate, "notifications": notes})


@app.post("/notifications")
def create_notification():
    """Direct path: {"to_user_id": 1, "type": "like", "from_user_id": 2, "post_id": 1}."""
    try:
        data = unmarshal(request.get_data(), g.request_id)
    except (ValueError, UnicodeDecodeError):
        return error("request body must be valid JSON", 400)
    if not isinstance(data, dict):
        return error("request body must be a JSON object", 400)
    if not valid_id(data.get("to_user_id")):
        return error("to_user_id must be a positive integer", 400)
    if data.get("type") not in VALID_TYPES:
        return error(f"type must be one of {sorted(VALID_TYPES)}", 400)

    # Only dedupe by a request ID the CALLER sent (not one we generated).
    caller_id = request.headers.get("X-Request-ID")
    note, duplicate = store.add_direct(caller_id, data["to_user_id"], data["type"],
                                       data.get("from_user_id"), data.get("post_id"))
    if duplicate:
        log.info("DUPLICATE request_id=%s, returning original", caller_id)
    return json_reply({"success": True, "duplicate": duplicate, "notification": note},
                      200 if duplicate else 201)


@app.get("/notifications/<int:user_id>")
def get_notifications(user_id):
    """Client asks for a user's notifications, newest first."""
    if not valid_id(user_id):
        return error("user_id must be positive", 400)
    notes = store.for_user(user_id)
    return json_reply({"success": True, "user_id": user_id, "notifications": notes})


# ---------------------------------------------------------------- startup
def subscribe(broker_url, callback_url):
    """Register for every topic in TOPICS. Same body format Feed uses."""
    for topic in TOPICS:
        body = {"topic": topic, "url": callback_url}
        wire = marshal(body, "SUBSCRIBE")
        response = requests.post(broker_url.rstrip("/") + "/subscribe", data=wire,
                                 headers={"Content-Type": "application/json",
                                          "X-Request-ID": str(uuid.uuid4())},
                                 timeout=2)
        result = unmarshal(response.content, "SUBSCRIBE_REPLY")
        if not response.ok or result.get("success") is not True:
            raise RuntimeError(f"Broker refused subscription to {topic}: {result}")
        log.info("SUBSCRIBED topic=%s callback=%s", topic, callback_url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Notification service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5004)
    parser.add_argument("--profile-url", default="http://127.0.0.1:5001")
    parser.add_argument("--broker-url", help="e.g. http://127.0.0.1:5005")
    parser.add_argument("--callback-url",
                        help="URL the broker uses to reach our /events")
    args = parser.parse_args()
    PROFILE_URL = args.profile_url.rstrip("/")
    callback_url = args.callback_url or f"http://127.0.0.1:{args.port}/events"

    # Start HTTP BEFORE subscribing so we can accept events immediately.
    server = make_server(args.host, args.port, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("HTTP_LISTEN %s:%s", args.host, args.port)
    try:
        if args.broker_url:
            subscribe(args.broker_url, callback_url)
        else:
            log.info("No --broker-url given; only direct POST /notifications works")
        thread.join()
    except KeyboardInterrupt:
        log.info("Stopping Notification service")
    except (requests.RequestException, RuntimeError) as exc:
        log.error("Startup failed: %s", exc)
        raise SystemExit(1)
    finally:
        server.shutdown()
        server.server_close()
