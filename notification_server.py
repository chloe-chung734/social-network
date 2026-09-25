import logging
import os
import sys
import threading
from datetime import datetime, timezone
 
from flask import Flask, jsonify, request

PORT = 5004

VALID_TYPES = {"like", "comment", "follow", "new_post"}
 

# Logging: console + logs/notification.log
# Same format as client.py so the logs line up side by side in the demo.

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [notification] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("logs/notification.log")],
)
log = logging.getLogger("notification")
 
# Hide Flask's default per-request lines; we log our own SEND/RECV lines instead
logging.getLogger("werkzeug").setLevel(logging.WARNING)
 
app = Flask(__name__)
 
# In-memory storage (disappears on restart - fine for a prototype)

# user_id -> list of notification dicts for that user
_notifications = {}
 

_seen_requests = {}
 
# Next notification ID to hand out
_next_id = 1
 
# Flask handles requests on multiple threads. Two requests arriving at the same
# moment could both read _next_id before either increments it, giving two
# notifications the same ID. The lock makes each write happen one at a time.
_lock = threading.Lock()
 
 

# Storage helpers
# this will be swapped with a real database later
def store_notification(to_user_id, ntype, from_user_id, post_id):
    """Create a notification, save it under the recipient, and return it."""
    global _next_id
    note = {
        "id": _next_id,
        "to_user_id": to_user_id,
        "type": ntype,
        "from_user_id": from_user_id,
        "post_id": post_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _next_id += 1
    # setdefault creates an empty list the first time we see this user
    _notifications.setdefault(to_user_id, []).append(note)
    return note
 
 
def retrieve_notifications(user_id):
    """Return a user's notifications, newest first (empty list if none)."""
    return list(reversed(_notifications.get(user_id, [])))
 
 
#Routes

@app.post("/notifications")
def create_notification():
    """Called by the Post server when something happens that a user should see.
 
    Expected JSON body:
        {"to_user_id": 1, "type": "like", "from_user_id": 2, "post_id": 1}
    """
    request_id = request.headers.get("X-Request-ID")
    data = request.get_json(silent=True)  # None instead of crashing on bad JSON
    log.info("RECV  <- POST /notifications %s (request_id=%s)", data, request_id)
 
    # --- Duplicate check: have we already handled this exact request? ---
    with _lock:
        if request_id and request_id in _seen_requests:
            log.info("DUPLICATE request_id=%s, returning original response", request_id)
            return jsonify(_seen_requests[request_id]), 200
 
    # --- Validate the input before storing anything ---
    if not data:
        return _error(400, "request body must be JSON")
    if not isinstance(data.get("to_user_id"), int):
        return _error(400, "to_user_id (int) is required")
    if data.get("type") not in VALID_TYPES:
        return _error(400, f"type must be one of {sorted(VALID_TYPES)}")
 
    # --- Store it (inside the lock so IDs stay unique) ---
    with _lock:
        note = store_notification(
            to_user_id=data["to_user_id"],
            ntype=data["type"],
            from_user_id=data.get("from_user_id"),
            post_id=data.get("post_id"),
        )
        if request_id:
            _seen_requests[request_id] = note  # remember for future retries
 
    log.info("SEND  -> 201 %s", note)
    return jsonify(note), 201
 
 
@app.get("/notifications/<int:user_id>")
def get_notifications(user_id):
    """Called by the client to see a user's notifications.
 
    <int:user_id> makes Flask convert "1" in the URL to the integer 1,
    so it matches the integer keys we stored under.
    """
    log.info("RECV  <- GET /notifications/%d", user_id)
    notes = retrieve_notifications(user_id)
    log.info("SEND  -> 200 (%d notifications)", len(notes))
    return jsonify(notes), 200
 
 
def _error(status, message):
    """Log and return a JSON error with a real HTTP status code."""
    log.info("SEND  -> %d %s", status, message)
    return jsonify({"error": message}), status
 
 
if __name__ == "__main__":
    log.info("Notification server listening on port %d", PORT)
    # threaded=True is the default; stated here because it's why we need _lock
    app.run(host="localhost", port=PORT, threaded=True)
 
# tests: 

"""
run the server with:
python notification_server.py


in another terminal test with curl to say user 2 likeed user 1's post:

curl -X POST localhost:5004/notifications -H "Content-Type: application/json" -H "X-Request-ID: abc" -d '{"to_user_id":1,"type":"like","from_user_id":2,"post_id":1}'

check user one's notifications:

curl localhost:5004/notifications/1


user 2 comments on user 1's post:

curl -X POST localhost:5004/notifications -H "Content-Type: application/json" -H "X-Request-ID: xyz" -d '{"to_user_id":1,"type":"comment","from_user_id":2,"post_id":1}'




curl localhost:5004/notifications/1


"""
