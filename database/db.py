import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "social_network.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            bio TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS follows (
            follower_id INTEGER NOT NULL,
            following_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (follower_id, following_id),
            FOREIGN KEY (follower_id) REFERENCES users (user_id) ON DELETE CASCADE,
            FOREIGN KEY (following_id) REFERENCES users (user_id) ON DELETE CASCADE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            post_id INTEGER PRIMARY KEY AUTOINCREMENT,
            author_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            caption TEXT DEFAULT '',
            timestamp TEXT NOT NULL,
            FOREIGN KEY (author_id) REFERENCES users (user_id) ON DELETE CASCADE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS likes (
            user_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, post_id),
            FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
            FOREIGN KEY (post_id) REFERENCES posts (post_id) ON DELETE CASCADE
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            reference_id TEXT,
            is_read INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (recipient_id) REFERENCES users (user_id) ON DELETE CASCADE
        )
    ''')

    conn.commit()
    conn.close()

def create_user(user_id: int, username: str, bio: str = ""):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, username, bio) VALUES (?, ?, ?)",
        (int(user_id), username, bio)
    )
    conn.commit()
    conn.close()

def add_follow_relationship(follower_id: int, following_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO follows (follower_id, following_id) VALUES (?, ?)",
        (int(follower_id), int(following_id))
    )
    conn.commit()
    conn.close()

def get_user_following(user_id: int) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT following_id FROM follows WHERE follower_id = ?", (int(user_id),))
    rows = cursor.fetchall()
    conn.close()
    return [int(row["following_id"]) for row in rows]

def create_post(author_id: int, content: str, caption: str = "", timestamp: str = None) -> dict:
    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO posts (author_id, content, caption, timestamp) VALUES (?, ?, ?, ?)",
        (int(author_id), content, caption, timestamp)
    )
    post_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return {
        "id": int(post_id),
        "author": int(author_id),
        "timestamp": timestamp,
        "content": content,
        "caption": caption,
        "likes": [],
        "comments": []
    }

def get_posts_by_author(author_id: int) -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM posts WHERE author_id = ? ORDER BY timestamp DESC", (int(author_id),))
    rows = cursor.fetchall()
    conn.close()

    posts = []
    for row in rows:
        posts.append({
            "id": int(row["post_id"]),
            "author": int(row["author_id"]),
            "timestamp": row["timestamp"],
            "content": row["content"],
            "caption": row["caption"],
            "likes": [],
            "comments": []
        })
    return posts

if __name__ == "__main__":
    init_db()