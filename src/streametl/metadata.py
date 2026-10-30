import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
 run_id TEXT PRIMARY KEY, source_file TEXT NOT NULL, status TEXT NOT NULL,
 started_at TEXT NOT NULL, ended_at TEXT, input_records INTEGER NOT NULL DEFAULT 0,
 inserted_records INTEGER NOT NULL DEFAULT 0, duplicate_records INTEGER NOT NULL DEFAULT 0,
 rejected_records INTEGER NOT NULL DEFAULT 0, failure_reason TEXT
);
CREATE TABLE IF NOT EXISTS events (
 event_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, order_id TEXT NOT NULL,
 customer_id TEXT NOT NULL, event_type TEXT NOT NULL, amount TEXT NOT NULL,
 currency TEXT NOT NULL, event_timestamp TEXT NOT NULL, event_date TEXT NOT NULL,
 source_file TEXT NOT NULL, source_row INTEGER NOT NULL, first_run_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_order_id ON events(order_id);
CREATE INDEX IF NOT EXISTS idx_events_event_date ON events(event_date);
"""

def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
