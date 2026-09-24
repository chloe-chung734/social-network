import sqlite3
import os

# Database file location
DB_PATH = os.path.join(os.path.dirname(__file__), "social_network.db")

def get_connection():
    """Establishes and returns a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")  
    return conn

def init_db():
    """Initializes the database schema for all microservices."""
    conn = get_connection()
    cursor = conn.cursor()

    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            bio TEXT DEFAULT '',
            profile_pic TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            post_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            content TEXT NOT NULL,
            image_url TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
        )
    ''')

    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS follows (
            follower_id TEXT NOT NULL,
            following_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (follower_id, following_id),
            FOREIGN KEY (follower_id) REFERENCES users (user_id) ON DELETE CASCADE,
            FOREIGN KEY (following_id) REFERENCES users (user_id) ON DELETE CASCADE
        )
    ''')

    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS likes (
            user_id TEXT NOT NULL,
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
            recipient_id TEXT NOT NULL,
            sender_id TEXT NOT NULL,
            type TEXT NOT NULL, -- 'LIKE', 'COMMENT', 'FOLLOW'
            reference_id TEXT,
            is_read INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (recipient_id) REFERENCES users (user_id) ON DELETE CASCADE
        )
    ''')

    conn.commit()
    conn.close()
    print("[Database] Schema initialized successfully.")



def create_user(user_id, username, bio=""):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, username, bio) VALUES (?, ?, ?)",
        (user_id, username, bio)
    )
    conn.commit()
    conn.close()

def create_post(user_id, content, image_url=""):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO posts (user_id, content, image_url) VALUES (?, ?, ?)",
        (user_id, content, image_url)
    )
    post_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return post_id

def get_user_feed(user_id):
    """Fetch posts from followed users for Feed Server."""
    conn = get_connection()
    cursor = conn.cursor()
    query = '''
        SELECT p.post_id, p.user_id, u.username, p.content, p.image_url, p.created_at
        FROM posts p
        JOIN users u ON p.user_id = u.user_id
        JOIN follows f ON p.user_id = f.following_id
        WHERE f.follower_id = ?
        ORDER BY p.created_at DESC
    '''
    cursor.execute(query, (user_id,))
    posts = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return posts

if __name__ == "__main__":
    init_db()
