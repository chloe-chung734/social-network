from datetime import datetime, timezone

from flask import Flask, jsonify, request

import requests


NOTIFICATION_SERVER_URL = "http://127.0.0.1:8003/notifications"


app = Flask(__name__)


class Post:
    def __init__(self, post_id, author, timestamp, content, caption):
        self.id = post_id
        self.author = author
        self.timestamp = timestamp
        self.content = content
        self.caption = caption

        # Stores user IDs who liked this post
        self.likes = []

        # Stores Comment objects belonging to this post
        self.comments = []

    def to_dict(self):
        return {
            "id": self.id,
            "author": self.author,
            "timestamp": self.timestamp,
            "content": self.content,
            "caption": self.caption,
            "likes": self.likes,
            "comments": [
                comment.to_dict()
                for comment in self.comments
            ]
        }


class Comment:
    def __init__(self, comment_id, author, content):
        self.id = comment_id
        self.author = author
        self.content = content

    def to_dict(self):
        return {
            "id": self.id,
            "author": self.author,
            "content": self.content
        }


# Temporary in-memory storage.
# This can later be replaced with database repository functions.
posts = {}

next_post_id = 1
next_comment_id = 1


@app.post("/posts/create")
def create_post():
    global next_post_id

    try:
        data = request.get_json(silent=True) or {}

        required_fields = ["author", "content", "caption"]

        for field in required_fields:
            if field not in data:
                return jsonify({
                    "success": False,
                    "error": f"Missing required field: {field}"
                }), 400

        post = Post(
            post_id=next_post_id,
            author=data["author"],
            timestamp=datetime.now(timezone.utc).isoformat(),
            content=data["content"],
            caption=data["caption"]
        )

        posts[post.id] = post
        next_post_id += 1

        notification_sent = notify_notification_server(post)

        return jsonify({
            "success": True,
            "post": post.to_dict(),
            "notification_sent": notification_sent
        }), 201

    except Exception as error:
        return jsonify({
            "success": False,
            "error": str(error)
        }), 400


@app.post("/posts/retrieve")
def retrieve_posts():
    try:
        data = request.get_json(silent=True) or {}

        if "author" not in data:
            return jsonify({
                "success": False,
                "error": "Missing required field: author"
            }), 400

        requested_author = data["author"]

        author_posts = [
            post.to_dict()
            for post in posts.values()
            if post.author == requested_author
        ]

        return jsonify({
            "success": True,
            "posts": author_posts
        }), 200

    except Exception as error:
        return jsonify({
            "success": False,
            "error": str(error)
        }), 400


@app.post("/posts/<int:post_id>/comments")
def create_comment(post_id):
    global next_comment_id

    post = posts.get(post_id)

    if post is None:
        return jsonify({
            "success": False,
            "error": "Post not found"
        }), 404

    data = request.get_json(silent=True) or {}

    author = data.get("author")
    content = data.get("content")

    if not author or not content:
        return jsonify({
            "success": False,
            "error": "author and content are required"
        }), 400

    comment = Comment(
        comment_id=next_comment_id,
        author=author,
        content=content
    )

    post.comments.append(comment)
    next_comment_id += 1

    return jsonify({
        "success": True,
        "comment": comment.to_dict(),
        "post": post.to_dict()
    }), 201


@app.delete("/posts/<int:post_id>")
def delete_post(post_id):
    post = posts.get(post_id)

    if post is None:
        return jsonify({
            "success": False,
            "error": "Post not found"
        }), 404

    deleted_post = post.to_dict()

    del posts[post_id]

    return jsonify({
        "success": True,
        "deleted_post": deleted_post
    }), 200


@app.delete("/posts/<int:post_id>/comments/<int:comment_id>")
def delete_comment(post_id, comment_id):
    post = posts.get(post_id)

    if post is None:
        return jsonify({
            "success": False,
            "error": "Post not found"
        }), 404

    comment_to_delete = None

    for comment in post.comments:
        if comment.id == comment_id:
            comment_to_delete = comment
            break

    if comment_to_delete is None:
        return jsonify({
            "success": False,
            "error": "Comment not found"
        }), 404

    deleted_comment = comment_to_delete.to_dict()

    post.comments.remove(comment_to_delete)

    return jsonify({
        "success": True,
        "deleted_comment": deleted_comment,
        "post": post.to_dict()
    }), 200


@app.post("/posts/<int:post_id>/likes/<string:user_id>")
def like_post(post_id, user_id):
    post = posts.get(post_id)

    if post is None:
        return jsonify({
            "success": False,
            "error": "Post not found"
        }), 404

    if user_id in post.likes:
        return jsonify({
            "success": False,
            "error": "User has already liked this post"
        }), 409

    post.likes.append(user_id)

    return jsonify({
        "success": True,
        "message": "Post liked successfully",
        "post": post.to_dict()
    }), 201


@app.delete("/posts/<int:post_id>/likes/<string:user_id>")
def unlike_post(post_id, user_id):
    post = posts.get(post_id)

    if post is None:
        return jsonify({
            "success": False,
            "error": "Post not found"
        }), 404

    if user_id not in post.likes:
        return jsonify({
            "success": False,
            "error": "User has not liked this post"
        }), 404

    post.likes.remove(user_id)

    return jsonify({
        "success": True,
        "message": "Like deleted successfully",
        "post": post.to_dict()
    }), 200


def notify_notification_server(post):
    notification_message = {
        "type": "new_post",
        "post_id": post.id,
        "author": post.author,
        "timestamp": post.timestamp
    }

    try:
        response = requests.post(
            NOTIFICATION_SERVER_URL,
            json=notification_message,
            timeout=5
        )

        response.raise_for_status()

        return True

    except requests.RequestException as error:
        print(f"Notification failed: {error}")
        return False


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=8001,
        debug=True
    )