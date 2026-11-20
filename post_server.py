"""Post HTTP service with authoritative post records stored through Chord."""

import hashlib
import json
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
from flask import Flask, current_app, g, jsonify, request
from werkzeug.serving import make_server

from chord_client import (ChordClient, ChordError, ChordOutcomeUnknown,
                          ChordTimeout, ChordUnavailable)
from message_protocol import MessageContext, MessageProtocolError


DEFAULT_BROKER_URL = "http://127.0.0.1:5005"
AUTHOR_BUDGET_SECONDS = 7.0
_message_context = None
_client = ChordClient()
# Every read-modify-write of Chord records (ID counter, author index, likes,
# comments) runs under this lock. Flask is threaded, and without it two
# requests can read the same counter or post and overwrite each other.
# This serializes this process; the ZooKeeper-backed grant serializes all Post
# processes when coordination mode is enabled.
_write_lock = threading.Lock()

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024


def make_logger():
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("post")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)sZ [post] %(message)s",
                                      "%Y-%m-%dT%H:%M:%S")
        formatter.converter = time.gmtime
        for handler in (logging.StreamHandler(),
                        logging.FileHandler("logs/post.log", encoding="utf-8")):
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    return logger


log = make_logger()


def valid_id(value):
    return type(value) is int and value > 0


def error_response(message, status):
    grant = getattr(g, "coordination_grant", None)
    if status >= 400 and grant is not None and not getattr(g, "write_rejected_logged", False):
        _context().log("WRITE_REJECTED", g.request_id, resource="post-write",
                       grant_token=grant.get("token"), status=status)
        g.write_rejected_logged = True
    return jsonify({"success": False, "error": message}), status


def process_context():
    """Return this process's single shared clock and message identity."""
    global _message_context
    if _message_context is None:
        node_name = os.getenv("NODE_NAME")
        if not node_name:
            node_name = "post-b" if os.getenv("POST_PORT", "5002") == "5006" else "post-a"
        _message_context = MessageContext(node_name)
    return _message_context


_UNSET = object()


def create_app(*, message_context=None, chord_client=None,
               coordination_client=_UNSET):
    """Configure the Post Flask app, sharing one context with helper clients.

    Coordination is off by default for Milestone 3. If enabled, failure to
    initialize the discovery client is fatal; there is no local-lock fallback.
    """
    global _client
    ctx = message_context or process_context()
    client = chord_client or ChordClient(message_context=ctx)
    client.message_context = ctx
    enabled = os.getenv("COORDINATION_ENABLED", "0") == "1"
    if coordination_client is _UNSET:
        coord = None
        if enabled:
            try:
                from coordination_client import CoordinationClient
                coord = CoordinationClient(
                    ctx, hosts=os.getenv("ZK_HOSTS", "127.0.0.1:2181"),
                    root=os.getenv("COORD_ROOT", "/social-network/m4"))
            except Exception as exc:
                raise RuntimeError(
                    "COORDINATION_ENABLED=1 but coordinator discovery failed: %s" % exc
                ) from exc
    else:
        coord = coordination_client
    if enabled and coord is None:
        raise RuntimeError("COORDINATION_ENABLED=1 requires distributed coordination")
    _client = client
    app.extensions["message_context"] = ctx
    app.extensions["chord_client"] = client
    app.extensions["coordination_client"] = coord
    return app


def _context():
    return current_app.extensions["message_context"]


def _chord():
    return current_app.extensions["chord_client"]


def _coordination():
    return current_app.extensions.get("coordination_client")


def _local_event(event, **fields):
    return _context().local_event(event, g.request_id, **fields)


@contextmanager
def protected_write_scope():
    """Hold the global grant and local lock for one complete mutation."""
    coord = _coordination()
    grant = None
    if coord is not None:
        try:
            grant = coord.acquire("post-write", g.request_id)
        except Exception as exc:
            _context().log("WRITE_REJECTED", g.request_id, resource="post-write",
                           reason="coordination_unavailable")
            raise RuntimeError("distributed coordination unavailable: %s" % exc) from exc
        g.coordination_grant = grant
        g.unresolved_write = False
        _local_event("CS_ENTER", resource="post-write", grant_token=grant["token"])
    _write_lock.acquire()
    try:
        yield grant
    finally:
        _write_lock.release()
        if coord is not None and grant is not None:
            if getattr(g, "unresolved_write", False):
                _context().log("WRITE_UNRESOLVED", g.request_id,
                               resource="post-write", grant_token=grant["token"])
            else:
                _local_event("CS_EXIT", resource="post-write",
                             grant_token=grant["token"])
                try:
                    coord.release(grant)
                except Exception as exc:
                    # The durable holder remains occupied if release is not
                    # confirmed; a later call can safely retry this grant.
                    _context().log("LOCK_RELEASE_PENDING", g.request_id,
                                   resource="post-write", grant_token=grant["token"],
                                   error=str(exc))
                    log.warning("LOCK_RELEASE_PENDING request_id=%s token=%s error=%s",
                                g.request_id, grant.get("token"), exc)


@app.errorhandler(RuntimeError)
def handle_runtime_error(exc):
    if "distributed coordination unavailable" in str(exc):
        return error_response("Coordination unavailable", 503)
    raise exc


def marshal(message, label):
    log.info("%s %r", label, message)
    return json.dumps(message, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


def read_json_object():
    try:
        data = json.loads(request.get_data().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, error_response("request body must be valid JSON", 400)
    if not isinstance(data, dict):
        return None, error_response("request body must be a JSON object", 400)
    return data, None


@app.before_request
def log_request():
    ctx = app.extensions.get("message_context") or process_context()
    app.extensions.setdefault("message_context", ctx)
    try:
        g.request_id = ctx.receive_headers(request.headers)
    except MessageProtocolError as exc:
        g.request_id = str(uuid.uuid4())
        g.message_metadata_error = True
        return error_response(str(exc), 400)
    log.info("RECV %s %s request_id=%s", request.method, request.path, g.request_id)


@app.after_request
def log_reply(response):
    ctx = app.extensions.get("message_context") or process_context()
    response.headers.update(ctx.send_headers(g.request_id))
    log.info("SEND status=%s request_id=%s", response.status_code, g.request_id)
    return response


def key_for(record_id):
    """Hash record IDs for metadata/index records; posts use decimal post IDs."""
    if record_id.startswith("post:"):
        return _client.routing_key(int(record_id.split(":", 1)[1]))
    digest = hashlib.sha256(record_id.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % 32


def load_record(record_id, deadline=None):
    value = _chord().get(record_id, key_for(record_id), g.request_id, deadline)
    if value is None:
        return None
    # The agreed value is the stored application JSON object.
    return value


def save_record(record_id, value, suffix="write", deadline=None):
    return _chord().put(record_id, key_for(record_id), value,
                        "%s:%s" % (g.request_id, suffix), deadline,
                        coordination=getattr(g, "coordination_grant", None))


def delete_record(record_id, suffix="delete", deadline=None):
    return _chord().delete(record_id, key_for(record_id),
                           "%s:%s" % (g.request_id, suffix), deadline,
                           coordination=getattr(g, "coordination_grant", None))


def storage_error(exc):
    log.warning("CHORD_FAILURE request_id=%s error=%s", g.request_id, exc)
    if isinstance(exc, ChordOutcomeUnknown):
        g.unresolved_write = True
        if not getattr(g, "write_rejected_logged", False):
            _context().log("WRITE_REJECTED", g.request_id, resource="post-write",
                           reason="storage_outcome_unknown")
            g.write_rejected_logged = True
        return error_response("Storage outcome is unknown; operation was not replayed", 504)
    _context().log("WRITE_REJECTED", g.request_id, resource="post-write",
                   reason="storage_failure")
    if isinstance(exc, ChordTimeout):
        return error_response("Chord storage timed out", 504)
    return error_response("Chord storage unavailable", 503)


def publish_event(event):
    event["event_id"] = str(uuid.uuid4())
    try:
        headers = _context().send_headers(g.request_id)
        response = requests.post(
            os.getenv("BROKER_URL", DEFAULT_BROKER_URL).rstrip("/") + "/publish",
            json=event, headers=headers, timeout=3.0)
        _context().receive_headers(response.headers)
        response.raise_for_status()
        result = response.json()
        return result.get("all_delivered") is True, result
    except (requests.RequestException, ValueError) as exc:
        log.warning("BROKER_DELIVERY_FAILED event_id=%s error=%s",
                    event["event_id"], exc)
        return False, str(exc)


def get_post(post_id, deadline=None):
    return load_record("post:%d" % post_id, deadline)


def update_post(post, suffix):
    save_record("post:%d" % post["id"], post, suffix)


def mark_write_complete():
    if _coordination() is not None:
        grant = g.coordination_grant
        _local_event("WRITE_COMPLETE", resource="post-write",
                     grant_token=grant["token"])


def get_author_index(author, deadline=None):
    record = load_record("author:%d" % author, deadline)
    if record is None:
        return {"post_ids": []}
    ids = record.get("post_ids") if isinstance(record, dict) else None
    if not isinstance(ids, list) or any(not valid_id(item) for item in ids):
        raise ChordUnavailable("invalid author index record")
    return {"post_ids": list(dict.fromkeys(ids))}


@app.get("/health")
def health():
    return jsonify({"service": "post", "status": "ok"})


@app.post("/posts")
def create_post():
    data, error = read_json_object()
    if error:
        return error
    author = data.get("user_id", data.get("author"))
    content = data.get("image_url", data.get("content", ""))
    caption = data.get("caption")
    if not valid_id(author):
        return error_response("author must be a positive integer", 400)
    if not isinstance(content, str) or not isinstance(caption, str):
        return error_response("content and caption must be strings", 400)

    with protected_write_scope():
        try:
            counter_id = "meta:next_post_id"
            counter = load_record(counter_id)
            post_id = counter.get("value", 1) if isinstance(counter, dict) else 1
            if not valid_id(post_id):
                raise ChordUnavailable("invalid post ID counter")
            save_record(counter_id, {"value": post_id + 1}, "counter")
            post = {"id": post_id, "author": author,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "content": content, "caption": caption,
                    "likes": [], "comments": []}
            save_record("post:%d" % post_id, post, "post")
            try:
                index = get_author_index(author)
                index["post_ids"] = sorted(set(index["post_ids"] + [post_id]))
                save_record("author:%d" % author, index, "author-index")
                mark_write_complete()
            except ChordOutcomeUnknown:
                raise
            except ChordError:
                # Best-effort compensation; the ID remains consumed.
                try:
                    delete_record("post:%d" % post_id, "compensate-post")
                except ChordOutcomeUnknown:
                    raise
                except ChordError:
                    pass
                raise
        except ChordError as exc:
            return storage_error(exc)
    reply = {"success": True, "id": post_id, "post": post,
             "event_published": False}
    if os.getenv("PUBLISH_POST_EVENTS", "1") == "1":
        delivered, result = publish_event({"type": "post.created", "post": post})
        reply["event_published"] = delivered
        if isinstance(result, dict):
            reply["event_delivery"] = result
        elif result:
            reply["event_publish_error"] = result
    return app.response_class(marshal(reply, g.request_id), status=201,
                              mimetype="application/json")


@app.post("/posts/retrieve")
def retrieve_posts():
    data, error = read_json_object()
    if error:
        return error
    author = data.get("author")
    if not valid_id(author):
        return error_response("author must be a positive integer", 400)
    deadline = time.monotonic() + AUTHOR_BUDGET_SECONDS
    try:
        index = get_author_index(author, deadline)
        posts = []
        for post_id in index["post_ids"]:
            post = get_post(post_id, deadline)
            if post is None:
                raise ChordUnavailable("indexed post %s is missing" % post_id)
            if post.get("author") != author:
                raise ChordUnavailable("indexed post has unexpected author")
            posts.append(post)
        posts.sort(key=lambda item: item.get("timestamp", ""), reverse=True)
    except ChordError as exc:
        return storage_error(exc)
    reply = {"success": True, "posts": posts}
    return app.response_class(marshal(reply, g.request_id), status=200,
                              mimetype="application/json")


@app.delete("/posts/<int:post_id>")
def delete_post(post_id):
    with protected_write_scope():
        try:
            post = get_post(post_id)
            if post is None:
                return error_response("Post not found", 404)
            delete_record("post:%d" % post_id, "delete-post")
            index = get_author_index(post["author"])
            index["post_ids"] = [item for item in index["post_ids"] if item != post_id]
            save_record("author:%d" % post["author"], index, "delete-index")
            mark_write_complete()
        except ChordError as exc:
            return storage_error(exc)
    return jsonify({"success": True, "deleted_post": post}), 200


@app.post("/posts/<int:post_id>/comments")
def create_comment(post_id):
    data, error = read_json_object()
    if error:
        return error
    author = data.get("user_id", data.get("author"))
    content = data.get("text", data.get("content"))
    if not valid_id(author):
        return error_response("author must be a positive integer", 400)
    if not isinstance(content, str) or not content:
        return error_response("content must be a nonempty string", 400)
    with protected_write_scope():
        try:
            post = get_post(post_id)
            if post is None:
                return error_response("Post not found", 404)
            # Comment IDs come from the stored post, so they stay unique
            # within the post even after Post restarts.
            existing = post.setdefault("comments", [])
            next_id = max((c.get("id", 0) for c in existing), default=0) + 1
            comment = {"id": next_id, "author": author, "content": content}
            post.setdefault("comments", []).append(comment)
            update_post(post, "comment")
            mark_write_complete()
        except ChordError as exc:
            return storage_error(exc)
    reply = {"success": True, "comment": comment, "post": post}
    if os.getenv("PUBLISH_POST_EVENTS", "1") == "1":
        delivered, result = publish_event({"type": "post.commented",
            "post_id": post_id, "post_author": post["author"], "user_id": author})
        reply["event_published"] = delivered
        if isinstance(result, dict):
            reply["event_delivery"] = result
        elif result:
            reply["event_publish_error"] = result
    return jsonify(reply), 201


@app.delete("/posts/<int:post_id>/comments/<int:comment_id>")
def delete_comment(post_id, comment_id):
    with protected_write_scope():
        try:
            post = get_post(post_id)
            if post is None:
                return error_response("Post not found", 404)
            comment = next((item for item in post.get("comments", [])
                            if item.get("id") == comment_id), None)
            if comment is None:
                return error_response("Comment not found", 404)
            post["comments"].remove(comment)
            update_post(post, "delete-comment")
            mark_write_complete()
        except ChordError as exc:
            return storage_error(exc)
    return jsonify({"success": True, "deleted_comment": comment, "post": post}), 200


@app.post("/posts/<int:post_id>/likes")
@app.post("/posts/<int:post_id>/like")
def like_post(post_id):
    data, error = read_json_object()
    if error:
        return error
    user_id = data.get("user_id")
    if not valid_id(user_id):
        return error_response("user_id must be a positive integer", 400)
    with protected_write_scope():
        try:
            post = get_post(post_id)
            if post is None:
                return error_response("Post not found", 404)
            if user_id in post.get("likes", []):
                return error_response("User has already liked this post", 409)
            post.setdefault("likes", []).append(user_id)
            update_post(post, "like")
            mark_write_complete()
        except ChordError as exc:
            return storage_error(exc)
    reply = {"success": True, "message": "Post liked successfully", "post": post}
    if os.getenv("PUBLISH_POST_EVENTS", "1") == "1":
        delivered, result = publish_event({"type": "post.liked",
            "post_id": post_id, "post_author": post["author"], "user_id": user_id})
        reply["event_published"] = delivered
        if isinstance(result, dict):
            reply["event_delivery"] = result
        elif result:
            reply["event_publish_error"] = result
    return jsonify(reply), 201


@app.delete("/posts/<int:post_id>/likes")
def unlike_post(post_id):
    data, error = read_json_object()
    if error:
        return error
    user_id = data.get("user_id")
    if not valid_id(user_id):
        return error_response("user_id must be a positive integer", 400)
    with protected_write_scope():
        try:
            post = get_post(post_id)
            if post is None:
                return error_response("Post not found", 404)
            if user_id not in post.get("likes", []):
                return error_response("User has not liked this post", 404)
            post["likes"].remove(user_id)
            update_post(post, "unlike")
            mark_write_complete()
        except ChordError as exc:
            return storage_error(exc)
    return jsonify({"success": True, "message": "Like removed successfully",
                    "post": post}), 200


create_app()


if __name__ == "__main__":
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
        coord = app.extensions.get("coordination_client")
        if coord is not None:
            coord.close()
