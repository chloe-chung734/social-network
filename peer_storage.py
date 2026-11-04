"""Local SQLite storage for ONE Chord peer (Milestone 3).

Each peer has its own file, <db_dir>/peer_<id>.db. Peers never share a
database; they only exchange records over HTTP (the peer server does that).

Every record has:
    record_id    "post:<n>", "author:<n>" or "meta:next_post_id" (the identity)
    routing_key  0..31, kept separate so two records may share a key
    value        the JSON object Post stored (the full post for post:<n>)
    version      owner write counter; a higher version always wins
    deleted      1 = deletion marker (tombstone) so old copies cannot return
    role         "primary" (this peer owns the key) or "backup"

When a write counts as successful (the peer server enforces this):
    the owner saves it here as primary AND its successor confirms saving the
    same record (same version) as backup. This module only saves what it gets.

How the peer calls this module:
    put / delete                  owner writes (assign the next version)
    get                           read one record (a tombstone has deleted=1)
    list_interval                 records with keys in (start, end] for a transfer
    import_records                save transferred or backup records (safe to repeat)
    set_role_for_interval, drop_interval, drop_ids, promote_backups_to_primary
"""

import json
import os
import sqlite3
from contextlib import contextmanager

from chord_utils import in_open_closed

TABLE = """
CREATE TABLE IF NOT EXISTS peer_records (
    record_id    TEXT PRIMARY KEY,
    routing_key  INTEGER NOT NULL,
    value_json   TEXT,
    version      INTEGER NOT NULL,
    deleted      INTEGER NOT NULL DEFAULT 0,
    role         TEXT NOT NULL CHECK(role IN ('primary', 'backup')),
    operation_id TEXT
)
"""


def _record(row):
    """One database row -> the dictionary peers send to each other."""
    return {"record_id": row["record_id"], "routing_key": row["routing_key"],
            "value": None if row["value_json"] is None else json.loads(row["value_json"]),
            "version": row["version"], "deleted": bool(row["deleted"]),
            "operation_id": row["operation_id"], "role": row["role"]}


class PeerStorage:
    def __init__(self, peer_id, db_dir="database"):
        self.peer_id = int(peer_id)
        os.makedirs(db_dir, exist_ok=True)
        self.db_path = os.path.join(db_dir, f"peer_{self.peer_id}.db")
        with self._write() as conn:
            conn.execute(TABLE)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_key ON peer_records(routing_key)")

    # connections

    @contextmanager
    def _write(self):
        """One write transaction; BEGIN IMMEDIATE stops two writers racing."""
        conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def _read(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _find(conn, record_id):
        return conn.execute("SELECT * FROM peer_records WHERE record_id = ?",
                            (record_id,)).fetchone()

    @staticmethod
    def _save(conn, record_id, key, value_json, version, deleted, role, operation_id):
        conn.execute("INSERT OR REPLACE INTO peer_records VALUES (?, ?, ?, ?, ?, ?, ?)",
                     (record_id, int(key), value_json, int(version), int(deleted),
                      role, operation_id))

    # owner writes

    def put(self, record_id, routing_key, value, operation_id=None):
        """Owner write. New version = old + 1. The same operation_id twice is a no-op."""
        with self._write() as conn:
            old = self._find(conn, record_id)
            if old is not None and operation_id is not None \
                    and old["operation_id"] == operation_id:
                return _record(old)
            version = old["version"] + 1 if old is not None else 1
            self._save(conn, record_id, routing_key, json.dumps(value), version, 0,
                       "primary", operation_id)
            return _record(self._find(conn, record_id))

    def delete(self, record_id, routing_key, operation_id=None):
        """Owner delete: keep a tombstone with a higher version, never just remove."""
        with self._write() as conn:
            old = self._find(conn, record_id)
            if old is not None and old["deleted"] and operation_id is not None \
                    and old["operation_id"] == operation_id:
                return _record(old)
            version = old["version"] + 1 if old is not None else 1
            self._save(conn, record_id, routing_key, None, version, 1, "primary",
                       operation_id)
            return _record(self._find(conn, record_id))

    def get(self, record_id):
        with self._read() as conn:
            row = self._find(conn, record_id)
        return _record(row) if row is not None else None

    # transfers and backups

    def list_interval(self, start, end, role=None):
        """Records (tombstones too) whose key is in the clockwise interval (start, end].

        start == end means the whole ring; start > end wraps past zero.
        """
        with self._read() as conn:
            if role is None:
                rows = conn.execute("SELECT * FROM peer_records ORDER BY record_id")
            else:
                rows = conn.execute("SELECT * FROM peer_records WHERE role = ? "
                                    "ORDER BY record_id", (role,))
            records = [_record(row) for row in rows.fetchall()]
        return [r for r in records if in_open_closed(r["routing_key"], start, end)]

    def import_records(self, records, role):
        """Save transferred records. Returns (applied, ignored).

        A copy is applied only if its version is newer than ours, so repeating
        a transfer does nothing and a delayed old copy cannot overwrite a newer
        record or bring back a deleted one. An equal-version copy sent as
        "primary" promotes our backup (the hand-off on join and leave).
        """
        applied = ignored = 0
        with self._write() as conn:
            for rec in records:
                old = self._find(conn, rec["record_id"])
                version = int(rec["version"])
                value_json = None if rec.get("value") is None else json.dumps(rec["value"])
                if old is None or version > old["version"]:
                    keep_primary = old is not None and old["role"] == "primary"
                    self._save(conn, rec["record_id"], rec["routing_key"], value_json,
                               version, 1 if rec.get("deleted") else 0,
                               "primary" if keep_primary else role,
                               rec.get("operation_id"))
                    applied += 1
                elif version == old["version"] and role == "primary" \
                        and old["role"] == "backup":
                    conn.execute("UPDATE peer_records SET role = 'primary' "
                                 "WHERE record_id = ?", (rec["record_id"],))
                    applied += 1
                else:
                    ignored += 1
        return applied, ignored

    def set_role_for_interval(self, start, end, role):
        """Change the role of every record whose key is in (start, end]."""
        ids = [r["record_id"] for r in self.list_interval(start, end)]
        with self._write() as conn:
            conn.executemany("UPDATE peer_records SET role = ? WHERE record_id = ?",
                             [(role, record_id) for record_id in ids])
        return len(ids)

    def drop_interval(self, start, end, role=None):
        """Remove local copies in (start, end]; call only after a confirmed hand-off."""
        ids = [r["record_id"] for r in self.list_interval(start, end, role)]
        return self.drop_ids(ids)

    def drop_ids(self, record_ids):
        with self._write() as conn:
            conn.executemany("DELETE FROM peer_records WHERE record_id = ?",
                             [(record_id,) for record_id in record_ids])
        return len(record_ids)

    def promote_backups_to_primary(self):
        """Crash takeover: the backups this peer holds become primary."""
        with self._write() as conn:
            return conn.execute("UPDATE peer_records SET role = 'primary' "
                                "WHERE role = 'backup'").rowcount

    def counts(self):
        with self._read() as conn:
            rows = conn.execute("SELECT role, deleted, COUNT(*) FROM peer_records "
                                "GROUP BY role, deleted").fetchall()
        result = {"primary": 0, "backup": 0, "tombstones": 0}
        for role, deleted, count in rows:
            result["tombstones" if deleted else role] += count
        return result
