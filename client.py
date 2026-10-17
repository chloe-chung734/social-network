"""
client.py - Client for the Social Network distributed prototype.

Sends HTTP requests to the Profile, Post, Feed, and Notification servers
and logs every request/response to the console and to logs/client.log.

Uses only the Python standard library (no pip install needed).

Usage:
    python client.py demo                          # run the full demo flow
    python client.py create-user 10 alice          # explicit positive user ID
    python client.py follow 11 10                  # user 11 follows user 10
    python client.py post 10 "Hello world" img.jpg
    python client.py like 1 11                     # use the returned post ID
    python client.py comment 1 11 "Nice pic!"       # use the returned post ID
    python client.py feed 11
    python client.py notifications 10

The demo uses author 10 and follower 11 by default. Existing matching profiles
are reused. Both Feed and Notification must subscribe to the running broker.
Use --author-id and --follower-id after demo to select other unused IDs.
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
import urllib.error
import urllib.request
from http.client import HTTPException

# ---------------------------------------------------------------------------
# Service addresses - change these to match the ports your servers run on.
# Each can also be overridden with an environment variable.
# ---------------------------------------------------------------------------
SERVICES = {
    "profile":      os.getenv("PROFILE_URL",      "http://127.0.0.1:5001"),
    "post":         os.getenv("POST_URL",         "http://127.0.0.1:5002"),
    "feed":         os.getenv("FEED_URL",         "http://127.0.0.1:5003"),
    "notification": os.getenv("NOTIFICATION_URL", "http://127.0.0.1:5004"),
}

TIMEOUT_SECONDS = 15  # Post can wait up to 10 seconds for broker delivery.
MAX_RETRIES = 2       # Retry GET requests only; writes may already have happened.

# ---------------------------------------------------------------------------
# Logging: console + logs/client.log
# ---------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)sZ] [client] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
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

    Request IDs correlate logs. They do not make writes idempotent: the current
    Post service does not deduplicate post/comment creation by request ID.
    """
    url = SERVICES[service].rstrip("/") + path
    request_id = str(uuid.uuid4())
    data = None
    if body is not None:
        log.info("BEFORE_MARSHAL request_id=%s message=%r", request_id, body)
        data = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        log.info("AFTER_MARSHAL request_id=%s wire=%r", request_id, data)
    headers = {"Content-Type": "application/json", "X-Request-ID": request_id}
    attempts = MAX_RETRIES + 1 if method == "GET" else 1

    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        log.info("SEND  -> %s-service %s %s request_id=%s", service, method,
                 path, request_id)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
                raw = resp.read()
                log.info("BEFORE_UNMARSHAL request_id=%s wire=%r", request_id, raw)
                result = json.loads(raw.decode("utf-8"))
                log.info("AFTER_UNMARSHAL request_id=%s message=%r", request_id, result)
                if not isinstance(result, dict):
                    raise ValueError("expected a JSON object")
                log.info("RECV  <- %s-service %d request_id=%s", service,
                         resp.status, request_id)
                return result
        except urllib.error.HTTPError as e:
            # Server answered with an error code - don't retry.
            msg = e.read().decode("utf-8", errors="replace")
            log.info("RECV  <- %s-service %d %s", service, e.code, msg)
            return {"error": msg, "status": e.code}
        except (UnicodeError, ValueError) as e:
            log.error("Invalid response from %s-service: %s", service, e)
            return {"error": f"{service}-service returned invalid JSON data: {e}"}
        except (urllib.error.URLError, OSError, HTTPException) as e:
            reason = getattr(e, "reason", e)
            log.info("FAIL  %s-service unreachable (attempt %d/%d): %s",
                     service, attempt, attempts, reason)
            if method != "GET":
                return {"error": "No confirmed response. The operation may have "
                        "completed; check the service logs before trying again.",
                        "request_id": request_id}

    log.info("GIVE UP  %s-service %s %s", service, method, path)
    return {"error": f"{service}-service unavailable"}


# ---------------------------------------------------------------------------
# Client operations (one per user action)
# ---------------------------------------------------------------------------
def create_user(user_id, username):
    return send_request("profile", "POST", "/profiles",
                        {"user_id": user_id, "username": username})

def follow(user_id, target_id):
    return send_request("profile", "POST", f"/profiles/{user_id}/following",
                        {"following_user_id": target_id})

def create_post(user_id, caption, image_url=""):
    return send_request("post", "POST", "/posts",
                        {"author": user_id, "caption": caption,
                         "content": image_url})

def like_post(post_id, user_id):
    return send_request("post", "POST", f"/posts/{post_id}/likes",
                        {"user_id": user_id})

def comment_post(post_id, user_id, text):
    return send_request("post", "POST", f"/posts/{post_id}/comments",
                        {"author": user_id, "content": text})

def get_feed(user_id):
    return send_request("feed", "GET", f"/feed/{user_id}")

def get_notifications(user_id):
    return send_request("notification", "GET", f"/notifications/{user_id}")


# ---------------------------------------------------------------------------
# Demo: exercises every service in one run
# ---------------------------------------------------------------------------
class DemoError(Exception):
    """Stop the demo at the first failed step so the result is clear."""


def require_success(result, step):
    if not isinstance(result, dict) or "error" in result or result.get("success") is False:
        raise DemoError(f"{step} failed: {result}")
    return result


def ensure_demo_profile(user_id, username):
    """Create a demo profile or reuse it, without overwriting another user."""
    profile = send_request("profile", "GET", f"/profiles/{user_id}")
    if profile.get("status") == 404:
        profile = create_user(user_id, username)
    require_success(profile, f"Prepare profile {user_id}")
    if profile.get("user_id") != user_id or profile.get("username") != username:
        raise DemoError(f"Profile {user_id} does not match demo user {username!r}. "
                        "Choose different --author-id and --follower-id values.")
    return profile


def require_delivery(result, step, minimum):
    """Check the actual broker report, including the number of recipients."""
    require_success(result, step)
    # Current Post uses event_publish_error for the create-post delivery report,
    # even on success. Likes/comments put the report in event_delivery instead.
    report = result.get("event_delivery")
    if not isinstance(report, dict):
        report = result.get("event_publish_error")
    if (result.get("event_published") is not True or not isinstance(report, dict)
            or report.get("all_delivered") is not True
            or type(report.get("delivered")) is not int
            or report["delivered"] < minimum):
        raise DemoError(f"{step} did not confirm delivery to {minimum} subscriber(s). "
                        "Check the broker and subscriber startup/logs. The write "
                        f"may already be stored. Delivery result: {report}")
    log.info("PUBSUB_CONFIRMED event_id=%s topic=%s delivered=%s",
             report.get("event_id"), report.get("topic"), report["delivered"])


def require_notification(result, ntype, from_user_id, post_id):
    require_success(result, f"Read {ntype} notifications")
    notes = result.get("notifications")
    if not isinstance(notes, list) or not any(
            isinstance(note, dict) and note.get("type") == ntype
            and note.get("from_user_id") == from_user_id
            and note.get("post_id") == post_id for note in notes):
        raise DemoError(f"Expected {ntype} notification for post {post_id} was missing.")


def run_demo(author_id=10, follower_id=11):
    log.info("===== DEMO START =====")
    if author_id == follower_id:
        raise DemoError("The author and follower must have different user IDs.")

    log.info("--- Step 1: prepare two profiles (Profile) ---")
    alice = ensure_demo_profile(author_id, "alice")
    bob = ensure_demo_profile(follower_id, "bob")
    alice_id, bob_id = alice["user_id"], bob["user_id"]

    log.info("--- Step 2: Bob follows Alice (Profile) ---")
    require_success(follow(bob_id, alice_id), "Follow Alice")

    log.info("--- Step 3: Alice creates a post (Post -> Broker -> subscribers) ---")
    result = require_success(create_post(alice_id, "Milestone 2 demo post!",
                                        "images/sunset.jpg"), "Create post")
    post = result.get("post")
    if not isinstance(post, dict) or type(post.get("id")) is not int or post["id"] <= 0:
        raise DemoError("Post response did not contain a valid post ID.")
    post_id = post["id"]
    require_delivery(result, "Publish post.created", minimum=2)
    follower_notes = get_notifications(bob_id)
    require_notification(follower_notes, "new_post", alice_id, post_id)

    log.info("--- Step 4: Bob likes and comments (Post -> Broker -> Notification) ---")
    require_delivery(like_post(post_id, bob_id), "Publish post.liked", minimum=1)
    require_delivery(comment_post(post_id, bob_id, "Great shot!"),
                     "Publish post.commented", minimum=1)

    log.info("--- Step 5: Bob loads his feed (Feed -> Profile + Post) ---")
    feed = require_success(get_feed(bob_id), "Read Bob's feed")
    posts = feed.get("posts")
    if not isinstance(posts, list) or not any(
            isinstance(item, dict) and item.get("id") == post_id
            and item.get("author") == alice_id for item in posts):
        raise DemoError(f"The new post {post_id} was missing from Bob's feed.")

    log.info("--- Step 6: Alice checks her like/comment notifications ---")
    notes = get_notifications(alice_id)
    require_notification(notes, "like", bob_id, post_id)
    require_notification(notes, "comment", bob_id, post_id)

    print("\nBob's feed:\n" + json.dumps(feed, indent=2))
    print("\nBob's new-post notifications:\n" + json.dumps(follower_notes, indent=2))
    print("\nAlice's notifications:\n" + json.dumps(notes, indent=2))
    log.info("DEMO PASSED author_id=%s follower_id=%s post_id=%s",
             alice_id, bob_id, post_id)
    return 0


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------
def positive_id(value):
    try:
        number = int(value)
        if number > 0:
            return number
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("IDs must be positive integers")


def main():
    p = argparse.ArgumentParser(description="Social Network client")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("demo")
    s.add_argument("--author-id", type=positive_id, default=10)
    s.add_argument("--follower-id", type=positive_id, default=11)
    s = sub.add_parser("create-user");   s.add_argument("user_id", type=positive_id); s.add_argument("username")
    s = sub.add_parser("follow");        s.add_argument("user_id", type=positive_id); s.add_argument("target_id", type=positive_id)
    s = sub.add_parser("post");          s.add_argument("user_id", type=positive_id); s.add_argument("caption"); s.add_argument("image_url", nargs="?", default="")
    s = sub.add_parser("like");          s.add_argument("post_id", type=positive_id); s.add_argument("user_id", type=positive_id)
    s = sub.add_parser("comment");       s.add_argument("post_id", type=positive_id); s.add_argument("user_id", type=positive_id); s.add_argument("text")
    s = sub.add_parser("feed");          s.add_argument("user_id", type=positive_id)
    s = sub.add_parser("notifications"); s.add_argument("user_id", type=positive_id)

    a = p.parse_args()
    if a.cmd == "demo":
        try:
            return run_demo(a.author_id, a.follower_id)
        except DemoError as error:
            log.error("DEMO FAILED: %s", error)
            return 1

    actions = {
        "create-user":   lambda: create_user(a.user_id, a.username),
        "follow":        lambda: follow(a.user_id, a.target_id),
        "post":          lambda: create_post(a.user_id, a.caption, a.image_url),
        "like":          lambda: like_post(a.post_id, a.user_id),
        "comment":       lambda: comment_post(a.post_id, a.user_id, a.text),
        "feed":          lambda: get_feed(a.user_id),
        "notifications": lambda: get_notifications(a.user_id),
    }
    result = actions[a.cmd]()
    print(json.dumps(result, indent=2))
    return 1 if "error" in result or result.get("success") is False else 0


if __name__ == "__main__":
    raise SystemExit(main())

