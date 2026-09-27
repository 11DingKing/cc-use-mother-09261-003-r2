"""SQLite 事件仓储：事件只追加、不可篡改、重启后可完整回放。

数据库层面三道硬保证：
1. UNIQUE(case_id, version) —— 并发写同一案例时只有一个事务能占上版本号；
2. one_publish_per_case 部分唯一索引 —— 同一案例全库最多一条 published 事件；
3. UPDATE/DELETE 触发器直接 RAISE(ABORT) —— 已发生的决定无法被覆盖或抹除。
"""
import json
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    type TEXT NOT NULL,
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (case_id, version)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_publish_per_case
    ON events (case_id) WHERE type = 'published';
CREATE UNIQUE INDEX IF NOT EXISTS idempotency_per_case
    ON events (case_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TRIGGER IF NOT EXISTS events_never_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS events_never_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;
"""


class SQLiteStore:
    """事件仓储：只提供追加与查询，不提供任何修改历史的入口。"""

    def __init__(self, path=":memory:"):
        self._lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self._lock:
            self.db.executescript(SCHEMA)
            self.db.commit()

    def append(self, event):
        """追加一条事件；违反唯一约束时抛出 sqlite3.IntegrityError。"""
        with self._lock:
            self.db.execute(
                "INSERT INTO events (case_id, version, type, actor, role, payload,"
                " idempotency_key, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (event["case_id"], event["version"], event["type"], event["actor"],
                 event["role"], json.dumps(event.get("payload") or {}, ensure_ascii=False),
                 event.get("idempotency_key"), event["created_at"]),
            )
            self.db.commit()

    def find_by_idempotency_key(self, case_id, key):
        """按幂等键查找已记录的事件，用于客户端重试去重。"""
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM events WHERE case_id = ? AND idempotency_key = ?",
                (case_id, key),
            ).fetchone()
        return self._to_dict(row) if row else None

    def events_of(self, case_id):
        """按版本顺序返回某案例的全部事件。"""
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM events WHERE case_id = ? ORDER BY version", (case_id,),
            ).fetchall()
        return [self._to_dict(r) for r in rows]

    def all_events(self):
        """按写入顺序返回全库事件，供重启后回放重建状态。"""
        with self._lock:
            rows = self.db.execute("SELECT * FROM events ORDER BY seq").fetchall()
        return [self._to_dict(r) for r in rows]

    @staticmethod
    def _to_dict(row):
        event = dict(row)
        event["payload"] = json.loads(event["payload"])
        return event
