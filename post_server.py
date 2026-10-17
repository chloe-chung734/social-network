"""Post service for the CECS 327 distributed social network prototype.

The service stores posts in memory. Data is lost when the process restarts.
Feed retrieves posts with POST /posts/retrieve.
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import requests
from flask import Flask, g, jsonify, request
from werkzeug.serving import make_server


DEFAULT_BROKER_URL = "http://127.0.0.1:5005"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024

# Keep the example simple: posts are stored in memory.
# The lock protects IDs and post data from simultaneous requests.
_lock = threading.Lock()
_posts = {}
_next_post_id = 1
_next_comment_id = 1


def make_logger():
    """Log to the terminal and logs/post.log with UTC timestamps."""
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("post")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)sZ [post] %(message)s", "%Y-%m-%dT%H:%M:%S"
        )
        formatter.converter = time.gmtime

        for handler in (
            logging.StreamHandler(),
            logging.FileHandler("logs/post.log", encoding="utf-8"),
        ):
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    return logger


log = make_logger()


def marshal(message, label):
    """Convert a Python value to JSON bytes and show both forms in the log."""
    log.info("%s BEFORE_MARSHAL %r", label, message)
    wire = json.dumps(
        message, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    log.info("%s AFTER_MARSHAL %r", label, wire)
    return wire


def unmarshal(wire, label):
    """Convert JSON bytes to a Python value and show both forms in the log."""
    log.info("%s BEFORE_UNMARSHAL %r", label, wire)
    message = json.loads(wire.decode("utf-8"))
    log.info("%s AFTER_UNMARSHAL %r", label, message)
    return message


def valid_id(value):
    """Accept positive integer IDs, but not booleans or numeric strings."""
    return type(value) is int and value > 0


def error_response(message, status):
    return jsonify({"success": False, "error": message}), status


class Post:
    def __init__(self, post_id, author, timestamp, content, caption):
        self.id = post_id
        self.author = author
        self.timestamp = timestamp
        self.content = content
        self.caption = caption
        self.likes = []
        self.comments = []

    def to_dict(self):
        return {
            "id": self.id,
            "author": self.author,
            "timestamp": self.timestamp,
            "content": self.content,
            "caption": self.caption,
            "likes": list(self.likes),
            "comments": [dict(comment) for comment in self.comments],
        }


@app.before_request
def log_request():
    """Preserve a supplied request ID or create one for this HTTP request."""
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    log.info("RECV %s %s request_id=%s",
             request.method, request.path, g.request_id)


@app.after_request
def log_reply(response):
    response.headers["X-Request-ID"] = g.request_id
    log.info("SEND status=%s request_id=%s body=%s",
             response.status_code, g.request_id,
             response.get_data(as_text=True))
    return response


def read_json_object():
    """Parse a JSON request body and return a helpful error if it is invalid."""
    try:
        data = unmarshal(request.get_data(), g.request_id)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, error_response("request body must be valid JSON", 400)

    if not isinstance(data, dict):
        return None, error_response("request body must be a JSON object", 400)

    return data, None


def publish_event(event):
    """Publish an event to the broker and report subscriber delivery results."""
    wire = marshal(event, "BROKER_EVENT")
    broker_url = os.getenv("BROKER_URL", DEFAULT_BROKER_URL).rstrip("/")
    request_id = str(uuid.uuid4())

    try:
        response = requests.post(
            broker_url + "/publish",
            data=wire,
            headers={
                "Content-Type": "application/json",
                "X-Request-ID": request_id,
            },
            # Allow for the broker's retries when a subscriber is slow.
            timeout=10,
        )
        log.info("BROKER_REPLY status=%s event_id=%s body=%s",
                 response.status_code, event["event_id"], response.text)
        response.raise_for_status()
        result = unmarshal(response.content, "BROKER_REPLY")
        delivered = result.get("all_delivered") is True
        if not delivered:
            log.warning("BROKER_PARTIAL_DELIVERY event_id=%s matched=%s delivered=%s failed=%s",
                        event["event_id"], result.get("matched"),
                        result.get("delivered"), result.get("failed"))
        return delivered, result
    except requests.RequestException as exc:
        # Keep the stored post even if the broker is unavailable.
        log.warning("BROKER_DELIVERY_FAILED event_id=%s error=%s",
                    event["event_id"], exc)
        return False, str(exc)


@app.get("/health")
def health():
    return jsonify({"service": "post", "status": "ok"})


@app.post("/posts")
def create_post():
    """Create a post from {"author": 7, "content": "...", "caption": "..."}."""
    global _next_post_id

    data, error = read_json_object()
    if error:
        return error

    # Accept both client-facing and Post service field names.
    author = data.get("user_id", data.get("author"))
    content = data.get("image_url", data.get("content", ""))
    caption = data.get("caption")

    if not valid_id(author):
        return error_response("author must be a positive integer", 400)
    if not isinstance(content, str):
        return error_response("content must be a string", 400)
    if not isinstance(caption, str):
        return error_response("caption must be a string", 400)

    with _lock:
        post = Post(
            post_id=_next_post_id,
            author=author,
            timestamp=datetime.now(timezone.utc).isoformat(),
            content=content,
            caption=caption,
        )
        _posts[post.id] = post
        _next_post_id += 1
        post_dict = post.to_dict()

    event_published = False
    publish_error = None
    if os.getenv("PUBLISH_POST_EVENTS", "1") == "1":
        event = {"event_id": str(uuid.uuid4()), "type": "post.created",
                 "post": post_dict}
        event_published, publish_error = publish_event(event)

    reply = {
        "success": True,
        "id": post_dict["id"],
        "post": post_dict,
        "event_published": event_published,
    }
    if publish_error:
        reply["event_publish_error"] = publish_error

    return app.response_class(
        marshal(reply, g.request_id),
        status=201,
        mimetype="application/json",
    )


@app.post("/posts/retrieve")
def retrieve_posts():
    """Feed calls this with {"author": 7}; return only that author's posts."""
    data, error = read_json_object()
    if error:
        return error

    author = data.get("author")
    if not valid_id(author):
        return error_response("author must be a positive integer", 400)

    with _lock:
        result_posts = [
            post.to_dict()
            for post in _posts.values()
            if post.author == author
        ]

    reply = {"success": True, "posts": result_posts}
    return app.response_class(
        marshal(reply, g.request_id),
        status=200,
        mimetype="application/json",
    )


@app.delete("/posts/<int:post_id>")
def delete_post(post_id):
    with _lock:
        post = _posts.pop(post_id, None)

    if post is None:
        return error_response("Post not found", 404)

    return jsonify({"success": True, "deleted_post": post.to_dict()}), 200


@app.post("/posts/<int:post_id>/comments")
def create_comment(post_id):
    """Add a comment from {"author": 2, "content": "Nice photo!"}."""
    global _next_comment_id

    data, error = read_json_object()
    if error:
        return error

    author = data.get("user_id", data.get("author"))
    content = data.get("text", data.get("content"))
    if not valid_id(author):
        return error_response("author must be a positive integer", 400)
    if not isinstance(content, str) or not content:
        return error_response("content must be a nonempty string", 400)

    with _lock:
        post = _posts.get(post_id)
        if post is None:
            return error_response("Post not found", 404)

        comment = {
            "id": _next_comment_id,
            "author": author,
            "content": content,
        }
        _next_comment_id += 1
        post.comments.append(comment)
        post_dict = post.to_dict()
        reply = {"success": True, "comment": comment, "post": post_dict}

    if os.getenv("PUBLISH_POST_EVENTS", "1") == "1":
        event = {"event_id": str(uuid.uuid4()), "type": "post.commented",
                 "post_id": post_id, "post_author": post_dict["author"],
                 "user_id": author}
        delivered, result = publish_event(event)
        reply["event_published"] = delivered
        if isinstance(result, dict):
            reply["event_delivery"] = result
        elif result:
            reply["event_publish_error"] = result

    return jsonify(reply), 201


@app.delete("/posts/<int:post_id>/comments/<int:comment_id>")
def delete_comment(post_id, comment_id):
    with _lock:
        post = _posts.get(post_id)
        if post is None:
            return error_response("Post not found", 404)

        comment = next(
            (item for item in post.comments if item["id"] == comment_id),
            None,
        )
        if comment is None:
            return error_response("Comment not found", 404)

        post.comments.remove(comment)
        reply = {
            "success": True,
            "deleted_comment": comment,
            "post": post.to_dict(),
        }

    return jsonify(reply), 200


@app.post("/posts/<int:post_id>/likes")
@app.post("/posts/<int:post_id>/like")
def like_post(post_id):
    """Like a post with a JSON body such as {"user_id": 2}."""
    data, error = read_json_object()
    if error:
        return error

    user_id = data.get("user_id")
    if not valid_id(user_id):
        return error_response("user_id must be a positive integer", 400)

    with _lock:
        post = _posts.get(post_id)
        if post is None:
            return error_response("Post not found", 404)
        if user_id in post.likes:
            return error_response("User has already liked this post", 409)

        post.likes.append(user_id)
        post_dict = post.to_dict()
        reply = {
            "success": True,
            "message": "Post liked successfully",
            "post": post_dict,
        }

    if os.getenv("PUBLISH_POST_EVENTS", "1") == "1":
        event = {"event_id": str(uuid.uuid4()), "type": "post.liked",
                 "post_id": post_id, "post_author": post_dict["author"],
                 "user_id": user_id}
        delivered, result = publish_event(event)
        reply["event_published"] = delivered
        if isinstance(result, dict):
            reply["event_delivery"] = result
        elif result:
            reply["event_publish_error"] = result

    return jsonify(reply), 201


@app.delete("/posts/<int:post_id>/likes")
def unlike_post(post_id):
    """Unlike a post with a JSON body such as {"user_id": 2}."""
    data, error = read_json_object()
    if error:
        return error

    user_id = data.get("user_id")
    if not valid_id(user_id):
        return error_response("user_id must be a positive integer", 400)

    with _lock:
        post = _posts.get(post_id)
        if post is None:
            return error_response("Post not found", 404)
        if user_id not in post.likes:
            return error_response("User has not liked this post", 404)

        post.likes.remove(user_id)
        reply = {
            "success": True,
            "message": "Like removed successfully",
            "post": post.to_dict(),
        }

    return jsonify(reply), 200


if __name__ == "__main__":
    # This host works for local testing. For other computers, use a reachable
    # interface such as 0.0.0.0 and configure the firewall as needed.
    host = os.getenv("POST_HOST", "127.0.0.1")
    port = int(os.getenv("POST_PORT", "5002"))
    server = make_server(host, port, app, threaded=True)
    log.info("HTTP_LISTEN %s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Stopping Post service")
    finally:
        server.server_close()
