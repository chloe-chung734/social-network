"""
client.py - Client for the Social Network distributed prototype.

Sends HTTP requests to the Profile, Post, Feed, and Notification servers
and logs every request/response to the console and to logs/client.log.

Uses only the Python standard library (no pip install needed).

Usage:
    python client.py demo                          # run the full demo flow
    python client.py create-user alice
    python client.py follow 1 2                    # user 1 follows user 2
    python client.py post 1 "Hello world" img.jpg
    python client.py like 1 2                      # post 1 liked by user 2
    python client.py comment 1 2 "Nice pic!"       # post 1, user 2 comments
    python client.py feed 2
    python client.py notifications 1
"""

import argparse
import json
import logging
import os
import sys
import uuid
import urllib.error
import urllib.request

# ---------------------------------------------------------------------------
# Service addresses - change these to match the ports your servers run on.
# Each can also be overridden with an environment variable.
# ---------------------------------------------------------------------------
SERVICES = {
    "profile":      os.getenv("PROFILE_URL",      "http://localhost:5001"),
    "post":         os.getenv("POST_URL",         "http://localhost:5002"),
    "feed":         os.getenv("FEED_URL",         "http://localhost:5003"),
    "notification": os.getenv("NOTIFICATION_URL", "http://localhost:5004"),
}

TIMEOUT_SECONDS = 3   # bounded timeout (semi-synchronous model)
MAX_RETRIES = 2       # retries on timeout / connection failure

# ---------------------------------------------------------------------------
# Logging: console + logs/client.log
# ---------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [client] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/client.log"),
    ],
)
log = logging.getLogger("client")


# ---------------------------------------------------------------------------
# Core request function
# ---------------------------------------------------------------------------
def send_request(service, method, path, body=None):
    """Send a JSON request to a service and return the parsed JSON response.

    Writes include a request ID so a retried request can be recognized as a
    duplicate by the server (idempotency, per the link-failure assumptions).
    """
    url = SERVICES[service] + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if method in ("POST", "PUT", "DELETE"):
        headers["X-Request-ID"] = str(uuid.uuid4())

    for attempt in range(1, MAX_RETRIES + 2):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        log.info("SEND  -> %s-service %s %s %s", service, method, path,
                 json.dumps(body) if body else "")
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                raw = resp.read().decode()
                result = json.loads(raw) if raw else {}
                log.info("RECV  <- %s-service %d %s", service, resp.status, raw)
                return result
        except urllib.error.HTTPError as e:
            # Server answered with an error code - don't retry.
            msg = e.read().decode()
            log.info("RECV  <- %s-service %d %s", service, e.code, msg)
            return {"error": msg, "status": e.code}
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            reason = getattr(e, "reason", e)
            log.info("FAIL  %s-service unreachable (attempt %d/%d): %s",
                     service, attempt, MAX_RETRIES + 1, reason)

    log.info("GIVE UP  %s-service %s %s", service, method, path)
    return {"error": f"{service}-service unavailable"}


# ---------------------------------------------------------------------------
# Client operations (one per user action)
# ---------------------------------------------------------------------------
def create_user(username):
    return send_request("profile", "POST", "/users", {"username": username})

def follow(user_id, target_id):
    return send_request("profile", "POST", f"/users/{user_id}/follow",
                        {"target_id": target_id})

def create_post(user_id, caption, image_url=""):
    return send_request("post", "POST", "/posts",
                        {"user_id": user_id, "caption": caption,
                         "image_url": image_url})

def like_post(post_id, user_id):
    return send_request("post", "POST", f"/posts/{post_id}/like",
                        {"user_id": user_id})

def comment_post(post_id, user_id, text):
    return send_request("post", "POST", f"/posts/{post_id}/comments",
                        {"user_id": user_id, "text": text})

def get_feed(user_id):
    return send_request("feed", "GET", f"/feed/{user_id}")

def get_notifications(user_id):
    return send_request("notification", "GET", f"/notifications/{user_id}")


# ---------------------------------------------------------------------------
# Demo: exercises every service in one run
# ---------------------------------------------------------------------------
def run_demo():
    log.info("===== DEMO START =====")

    log.info("--- Step 1: create two users (Profile) ---")
    alice = create_user("alice")
    bob = create_user("bob")
    alice_id = alice.get("id", 1)
    bob_id = bob.get("id", 2)

    log.info("--- Step 2: bob follows alice (Profile) ---")
    follow(bob_id, alice_id)

    log.info("--- Step 3: alice creates a post (Post) ---")
    post = create_post(alice_id, "First post!", "images/sunset.jpg")
    post_id = post.get("id", 1)

    log.info("--- Step 4: bob likes and comments (Post -> Notification) ---")
    like_post(post_id, bob_id)
    comment_post(post_id, bob_id, "Great shot!")

    log.info("--- Step 5: bob loads his feed (Feed -> Profile + Post) ---")
    feed = get_feed(bob_id)

    log.info("--- Step 6: alice checks notifications (Notification) ---")
    notes = get_notifications(alice_id)

    log.info("===== DEMO END =====")
    print("\nBob's feed:\n" + json.dumps(feed, indent=2))
    print("\nAlice's notifications:\n" + json.dumps(notes, indent=2))


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Social Network client")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("demo")
    s = sub.add_parser("create-user");   s.add_argument("username")
    s = sub.add_parser("follow");        s.add_argument("user_id", type=int); s.add_argument("target_id", type=int)
    s = sub.add_parser("post");          s.add_argument("user_id", type=int); s.add_argument("caption"); s.add_argument("image_url", nargs="?", default="")
    s = sub.add_parser("like");          s.add_argument("post_id", type=int); s.add_argument("user_id", type=int)
    s = sub.add_parser("comment");       s.add_argument("post_id", type=int); s.add_argument("user_id", type=int); s.add_argument("text")
    s = sub.add_parser("feed");          s.add_argument("user_id", type=int)
    s = sub.add_parser("notifications"); s.add_argument("user_id", type=int)

    a = p.parse_args()
    if a.cmd == "demo":
        run_demo()
        return

    actions = {
        "create-user":   lambda: create_user(a.username),
        "follow":        lambda: follow(a.user_id, a.target_id),
        "post":          lambda: create_post(a.user_id, a.caption, a.image_url),
        "like":          lambda: like_post(a.post_id, a.user_id),
        "comment":       lambda: comment_post(a.post_id, a.user_id, a.text),
        "feed":          lambda: get_feed(a.user_id),
        "notifications": lambda: get_notifications(a.user_id),
    }
    print(json.dumps(actions[a.cmd](), indent=2))


if __name__ == "__main__":
    main()
