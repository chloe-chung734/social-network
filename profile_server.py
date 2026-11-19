"""Profile service (port 5001): user profiles and the follow graph.

MILESTONE 4: VECTOR CLOCKS (Member 6)
    One MessageContext (Member 1's module) per process, created at import and
    shared by every Flask worker thread. Each incoming request is a receive
    event, merged BEFORE the handler runs; each response, errors included, is
    a send event with X-Node-ID / X-Request-ID / X-Vector-Clock headers.
    JSON bodies are unchanged. Requests without clock headers (curl, older
    callers) are still served and logged as UNTRACKED_INPUT; present but
    malformed clock headers get HTTP 400. Clock events are written to
    logs/profile.clock.jsonl (CLOCK_LOG_DIR changes the folder; NODE_NAME
    changes the node name).
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, g, jsonify, request

app = Flask(__name__)
PORT = 5001


def process_context(node_name="profile"):
    """Build this process's ONE MessageContext, or None in Milestone 3 mode.

    COORDINATION_ENABLED=1 refuses to run without the shared clock module,
    so the milestone demo can never silently lose clock metadata.
    """
    try:
        from message_protocol import MessageContext
    except ModuleNotFoundError as error:
        if error.name != "message_protocol":
            raise
        if os.getenv("COORDINATION_ENABLED", "0") == "1":
            raise RuntimeError("Milestone 4 requires Member 1's message_protocol.py") from error
        return None
    return MessageContext(os.getenv("NODE_NAME", node_name))


# The process clock. Handlers read this global at request time, so a test may
# swap in its own context; production code never creates a second one.
CONTEXT = process_context()

# Log timestamps in UTC.
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)sZ [profile_server] %(levelname)s %(message)s",
    "%Y-%m-%dT%H:%M:%S",
)
formatter.converter = time.gmtime
handler.setFormatter(formatter)

log = logging.getLogger("profile_server")
log.setLevel(logging.INFO)
log.addHandler(handler)
log.propagate = False

# In-memory starter data for local testing.
# User 2 follows users 7 and 9, as required by the shared scenario.
_profiles = {
    2: {"user_id": 2, "username": "user2", "display_name": "User 2", "bio": ""},
    7: {"user_id": 7, "username": "user7", "display_name": "User 7", "bio": ""},
    9: {"user_id": 9, "username": "user9", "display_name": "User 9", "bio": ""},
}
_following = {2: {7, 9}, 7: set(), 9: set()}
_lock = threading.Lock()


def valid_id(value):
    """Accept positive integer IDs, but reject booleans and strings."""
    return type(value) is int and value > 0


def current_request_id():
    """The request ID chosen in before_request (falls back for safety)."""
    return g.get("request_id") or request.headers.get("X-Request-ID", "-")


def log_json(label, value, request_id):
    """Show the JSON data being sent or received."""
    log.info(
        "%s request_id=%s timestamp=%s json=%s",
        label,
        request_id or "-",
        datetime.now(timezone.utc).isoformat(),
        json.dumps(value, separators=(",", ":"), sort_keys=True),
    )


@app.before_request
def log_request():
    supplied = request.headers.get("X-Request-ID")
    g.request_id = supplied or str(uuid.uuid4())
    if CONTEXT is not None:
        try:
            # Receive event: merge the caller's vector BEFORE handling.
            # Untracked input keeps its X-Request-ID (or gets a new UUID).
            g.request_id = CONTEXT.receive_headers(request.headers)
            CONTEXT.log("HTTP_RECV", g.request_id, method=request.method,
                        path=request.path)
        except ValueError as error:              # MessageProtocolError
            g.request_id = str(uuid.uuid4())     # the bad ID can't be echoed
            log.warning("Bad clock metadata path=%s error=%s", request.path, error)
            return jsonify({"success": False,
                            "error": f"Invalid clock metadata: {error}"}), 400
    request_id = g.request_id
    log.info(
        "HTTP request method=%s path=%s request_id=%s timestamp=%s",
        request.method,
        request.path,
        request_id,
        datetime.now(timezone.utc).isoformat(),
    )
    body = request.get_json(silent=True)
    if body is not None:
        log_json("JSON unmarshalled request", body, request_id)


@app.after_request
def log_response(response):
    response.headers["Content-Type"] = "application/json"
    request_id = g.get("request_id") or str(uuid.uuid4())
    response.headers["X-Request-ID"] = request_id
    if CONTEXT is not None:
        # Every reply (200/201/4xx/5xx) is a send event with fresh headers.
        sent = CONTEXT.send_headers(request_id)
        response.headers.update(sent)
        CONTEXT.log("HTTP_SEND", request_id, status=response.status_code,
                    path=request.path,
                    sent_vector_clock=json.loads(sent["X-Vector-Clock"]))
    log.info(
        "HTTP response status=%s request_id=%s body=%s",
        response.status_code,
        request_id,
        response.get_data(as_text=True),
    )
    return response


def profile_from(data, user_id, existing=None):
    """Build a profile, keeping old values when a PATCH omits fields."""
    existing = existing or {}
    return {
        "user_id": user_id,
        "username": data.get("username", existing.get("username")),
        "display_name": data.get("display_name", existing.get("display_name")),
        "bio": data.get("bio", existing.get("bio", "")),
    }


@app.get("/health")
def health():
    return jsonify({"success": True, "service": "profile"}), 200


@app.post("/profiles")
def create_profile():
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not valid_id(data.get("user_id")):
        return jsonify({"success": False, "error": "user_id must be a positive integer"}), 400
    if not isinstance(data.get("username"), str) or not data["username"].strip():
        return jsonify({"success": False, "error": "username is required"}), 400

    user_id = data["user_id"]
    with _lock:
        if user_id in _profiles:
            return jsonify({"success": False, "error": "profile already exists"}), 409
        profile = profile_from(data, user_id)
        _profiles[user_id] = profile
        _following.setdefault(user_id, set())

    log_json("JSON marshalled response", profile, current_request_id())
    return jsonify(profile), 201


@app.get("/profiles/<int:user_id>")
def read_profile(user_id):
    with _lock:
        profile = _profiles.get(user_id)

    if profile is None:
        return jsonify({"success": False, "error": "profile not found"}), 404

    log_json("JSON marshalled response", profile, current_request_id())
    return jsonify(profile), 200


@app.route("/profiles/<int:user_id>", methods=["PUT", "PATCH"])
def update_profile(user_id):
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not data:
        return jsonify({"success": False, "error": "JSON update data is required"}), 400

    if request.method == "PUT":
        required = ("username", "display_name", "bio")
        missing = [field for field in required if field not in data]
        if missing:
            return jsonify({"success": False, "error": f"PUT is missing fields: {missing}"}), 400

    with _lock:
        existing = _profiles.get(user_id)
        if existing is None:
            return jsonify({"success": False, "error": "profile not found"}), 404

        # The URL's ID is authoritative; a body can't change the profile ID.
        profile = profile_from(data, user_id, existing)
        _profiles[user_id] = profile

    log_json("JSON marshalled response", profile, current_request_id())
    return jsonify(profile), 200


@app.get("/profiles/<int:user_id>/following")
def get_following(user_id):
    """Feed's dependency endpoint."""
    with _lock:
        if user_id not in _profiles:
            return jsonify({
                "success": False,
                "error": "unknown user",
                "user_id": user_id,
            }), 404
        authors = sorted(_following.get(user_id, set()))

    # Feed checks these fields and expects integer IDs.
    body = {"user_id": user_id, "following": authors}
    log_json("JSON marshalled response", body, current_request_id())
    return jsonify(body), 200

@app.get("/profiles/<int:user_id>/followers")
def get_followers(user_id):
    """Notification's dependency: who follows this user?"""
    with _lock:
        # Unknown user -> 404
        if user_id not in _profiles:
            return jsonify({"success": False, "error": "unknown user",
                            "user_id": user_id}), 404
        # Check every user's following list and collect
        # the ones whose list contains user_id
        followers = sorted(uid for uid, targets in _following.items()
                           if user_id in targets)
    return jsonify({"user_id": user_id, "followers": followers}), 200

@app.post("/profiles/<int:user_id>/following")
def add_follow(user_id):
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not valid_id(data.get("following_user_id")):
        return jsonify({
            "success": False,
            "error": "following_user_id must be a positive integer",
        }), 400

    target_id = data["following_user_id"]
    with _lock:
        if user_id not in _profiles or target_id not in _profiles:
            return jsonify({"success": False, "error": "profile not found"}), 404
        _following.setdefault(user_id, set()).add(target_id)
        authors = sorted(_following[user_id])

    return jsonify({"success": True, "user_id": user_id, "following": authors}), 200


@app.delete("/profiles/<int:user_id>/following/<int:target_id>")
def remove_follow(user_id, target_id):
    with _lock:
        if user_id not in _profiles:
            return jsonify({"success": False, "error": "profile not found"}), 404
        _following.setdefault(user_id, set()).discard(target_id)
        authors = sorted(_following[user_id])

    return jsonify({"success": True, "user_id": user_id, "following": authors}), 200


if __name__ == "__main__":
    log.info("Starting Profile on port %s", PORT)
    app.run(host="127.0.0.1", port=PORT, threaded=True)