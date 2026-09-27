"""SQLite 事件仓储：只追加（append-only），已发生的决定不可修改。

并发安全保证：
* 写操作在 ``BEGIN IMMEDIATE`` 事务内完成，并由进程内锁串行化；
* ``(case_id, version)`` 唯一约束 + 调用方传入的 expected_version 实现乐观锁；
* Published 事件上的部分唯一索引保证同一案例在数据库层面最多发布一次；
* events 表上的触发器禁止任何 UPDATE / DELETE，历史只能被追加，不能被覆盖。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id          TEXT    NOT NULL,
    version          INTEGER NOT NULL,
    type             TEXT    NOT NULL,
    actor            TEXT    NOT NULL,
    role             TEXT    NOT NULL,
    payload          TEXT    NOT NULL,
    created_at       TEXT    NOT NULL,
    idempotency_key  TEXT,
    UNIQUE(case_id, version)
);

-- 同一案例最多存在一条 Published 事件，从数据库层杜绝两次互相冲突的发布。
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_publish_once
    ON events(case_id) WHERE type = 'Published';

CREATE TABLE IF NOT EXISTS idempotency_keys(
    idempotency_key TEXT PRIMARY KEY,
    case_id         TEXT NOT NULL,
    seq             INTEGER NOT NULL
);

-- 历史记录只读：任何改写或删除尝试都会被数据库拒绝。
CREATE TRIGGER IF NOT EXISTS trg_events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events 表仅允许追加，禁止修改已有决定');
END;

CREATE TRIGGER IF NOT EXISTS trg_events_no_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events 表仅允许追加，禁止删除已有决定');
END;
"""


class ConcurrencyConflict(Exception):
    """并发冲突：版本已被他人推进，或唯一约束被并发事务触发。"""


class SQLiteEventStore:
    """案例决定的唯一事实来源；重启后重放 events 表即可还原全部过程。"""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        # 并发写入时等待锁而不是立刻抛 SQLITE_BUSY
        self.db.execute("PRAGMA busy_timeout = 5000")
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript(SCHEMA)
        self.db.commit()

    def append(
        self,
        case_id: str,
        event_type: str,
        actor: str,
        role: str,
        payload: dict | None = None,
        *,
        expected_version: int | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[dict, bool]:
        """追加一条决定事件。

        expected_version 为该案例当前应有的最新版本号（新建案例传 0），
        事务内若发现实际版本不一致则抛 ConcurrencyConflict。
        返回 (事件字典, 是否为幂等重放)。
        """
        now = datetime.now(timezone.utc).isoformat()
        body = json.dumps(payload or {}, ensure_ascii=False)
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                if idempotency_key is not None:
                    hit = self.db.execute(
                        "SELECT seq FROM idempotency_keys WHERE idempotency_key = ?",
                        (idempotency_key,),
                    ).fetchone()
                    if hit is not None:
                        row = self.db.execute(
                            "SELECT * FROM events WHERE seq = ?", (hit["seq"],)
                        ).fetchone()
                        self.db.execute("COMMIT")
                        return self._to_dict(row), True

                current = self.db.execute(
                    "SELECT COALESCE(MAX(version), 0) AS v FROM events WHERE case_id = ?",
                    (case_id,),
                ).fetchone()["v"]
                if expected_version is not None and expected_version != current:
                    raise ConcurrencyConflict(
                        f"案例 {case_id} 版本已为 {current}，"
                        f"与期望版本 {expected_version} 不一致"
                    )

                cursor = self.db.execute(
                    """
                    INSERT INTO events(case_id, version, type, actor, role,
                                       payload, created_at, idempotency_key)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        case_id,
                        current + 1,
                        event_type,
                        actor,
                        role,
                        body,
                        now,
                        idempotency_key,
                    ),
                )
                seq = cursor.lastrowid
                if idempotency_key is not None:
                    self.db.execute(
                        "INSERT INTO idempotency_keys(idempotency_key, case_id, seq)"
                        " VALUES(?, ?, ?)",
                        (idempotency_key, case_id, seq),
                    )
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

            row = self.db.execute("SELECT * FROM events WHERE seq = ?", (seq,)).fetchone()
            return self._to_dict(row), False

    def find_by_idempotency_key(self, idempotency_key: str) -> dict | None:
        """按幂等键找回首次决定；不存在返回 None。"""
        with self._lock:
            row = self.db.execute(
                """
                SELECT e.* FROM events e
                JOIN idempotency_keys k ON k.seq = e.seq
                WHERE k.idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
        return self._to_dict(row) if row else None

    def events(self, case_id: str) -> list[dict]:
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM events WHERE case_id = ? ORDER BY seq ASC", (case_id,)
            ).fetchall()
        return [self._to_dict(r) for r in rows]

    def all_events(self) -> list[dict]:
        with self._lock:
            rows = self.db.execute("SELECT * FROM events ORDER BY seq ASC").fetchall()
        return [self._to_dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self.db.close()

    @staticmethod
    def _to_dict(row: sqlite3.Row) -> dict:
        data = dict(row)
        data["payload"] = json.loads(data["payload"])
        return data
