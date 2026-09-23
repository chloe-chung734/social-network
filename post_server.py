from datetime import datetime, timezone

from flask import Flask, jsonify, request

import requests


NOTIFICATION_SERVER_URL = "http://127.0.0.1:8003/notifications"


# Create the Flask application
app = Flask(__name__)


# Represents a comment on a post
class Comment:
    def __init__(self, comment_id, author, content):
        self.id = comment_id
        self.author = author
        self.content = content

    # Converts the Comment object into a dictionary
    # so it can be returned as JSON
    def to_dict(self):
        return {
            "id": self.id,
            "author": self.author,
            "content": self.content
        }


# Represents a social media post
class Post:
    def __init__(self, post_id, author, timestamp, content, caption):
        self.id = post_id
        self.author = author
        self.timestamp = timestamp
        self.content = content
        self.caption = caption

        # Stores the IDs of users who liked this post
        self.likes = []

        # Stores Comment objects belonging to this post
        self.comments = []

    # Converts the Post object into a dictionary
    # so it can be returned as JSON
    def to_dict(self):
        return {
            "id": self.id,
            "author": self.author,
            "timestamp": self.timestamp,
            "content": self.content,
            "caption": self.caption,
            "likes": self.likes,

            # Convert every Comment object into a dictionary
            "comments": [
                comment.to_dict()
                for comment in self.comments
            ]
        }


# Temporary in-memory storage for posts
# The posts will disappear when the server restarts
posts = {}


# Used to give each new post a unique ID
next_post_id = 1


# This function runs when the client sends a POST request
# to http://127.0.0.1:8001/posts/create
@app.post("/posts/create")
def create_post():
    global next_post_id

    try:
        # Read the JSON data sent by the client
        data = request.get_json()

        # Create a new Post object
        post = Post(
            post_id=next_post_id,
            author=data["author"],
            timestamp=datetime.now(timezone.utc).isoformat(),
            content=data["content"],
            caption=data["caption"]
        )

        # Store the post
        posts[post.id] = post
        next_post_id += 1

        # Notify the Notification Server
        notification_sent = notify_notification_server(post)

        # Send a success response
        return jsonify({
            "success": True,
            "post": post.to_dict(),
            "notification_sent": notification_sent
        }), 201

    except Exception as error:
        # Send a failure response if post creation fails
        return jsonify({
            "success": False,
            "error": str(error)
        }), 400


# This route receives a request from the Feed Server
# asking for all posts made by a specific author
@app.post("/posts/retrieve")
def retrieve_posts():
    try:
        # Read the JSON message from the Feed Server
        data = request.get_json()

        # Get the author whose posts are being requested
        requested_author = data["author"]

        # Find every post belonging to that author
        author_posts = [
            post.to_dict()
            for post in posts.values()
            if post.author == requested_author
        ]

        # Return the posts to the Feed Server
        return jsonify({
            "success": True,
            "posts": author_posts
        }), 200

    except Exception as error:
        # Return a failure response if retrieval fails
        return jsonify({
            "success": False,
            "error": str(error)
        }), 400

def notify_notification_server(post):
    # Message sent to the Notification Server
    notification_message = {
        "type": "new_post",
        "post_id": post.id,
        "author": post.author,
        "timestamp": post.timestamp
    }

    try:
        # Send the message to the Notification Server
        response = requests.post(
            NOTIFICATION_SERVER_URL,
            json=notification_message,
            timeout=5
        )

        # Raise an error if the Notification Server returns
        # a failure status code
        response.raise_for_status()

        return True

    except requests.RequestException as error:
        # The post was already created, so just report that
        # notification delivery failed
        print(f"Notification failed: {error}")
        return False

# Start the Flask server when this file is run directly
if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=8001,
        debug=True
    )
