# Social Network - Distributed Architecture Prototype

## Component Architecture
* **Profile Server:** Manages user accounts and social graph.
* **Post Server:** Handles post creation, likes, and comments.
* **Feed Server:** Aggregates and sorts user chronologically.
* **Notification Server:** Delivers real-time interaction alerts.
* **Database Layer (`database/db.py`):** SQLite persistence layer sharing tables across services.

---

## Database Setup & Execution

### Prerequisites
* Python 3.8+
* `sqlite3` (built-in with Python standard library)

### Initialize Database Schema
To set up or re-initialize the relational database schema, run the `db.py` script from the project root:

```bash
python database/db.py
