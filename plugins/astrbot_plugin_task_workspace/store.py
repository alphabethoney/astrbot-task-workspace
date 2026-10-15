# -*- coding: utf-8 -*-
"""SQLite-backed transactional state store.

All blocking DB work runs in a worker thread (``asyncio.to_thread``) and is
serialised by an ``RLock``. Write transactions are short and use
``BEGIN IMMEDIATE``. A partial unique index guarantees at most one active run
per workspace even across processes/reloads. Terminal writes and their outbox
rows commit atomically. A closed store never silently reopens.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path

from .models import (
    ACTIVE,
    ACTIVE_SQL,
    TERMINAL,
    OutboxItem,
    PendingRequirement,
    Run,
    RunStatus,
    Workspace,
)

_OUTBOX_BACKOFF = (5, 15, 60, 180, 600)

MIGRATIONS: list[tuple[int, list[str]]] = [
    (
        1,
        [
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_ts REAL NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS workspaces (
                id TEXT PRIMARY KEY,
                id_prefix TEXT NOT NULL UNIQUE,
                owner_user_id TEXT NOT NULL,
                umo TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                title TEXT NOT NULL,
                persona TEXT NOT NULL DEFAULT '',
                memory_policy TEXT NOT NULL DEFAULT 'isolated',
                cwd TEXT NOT NULL DEFAULT '',
                created_ts REAL NOT NULL,
                updated_ts REAL NOT NULL,
                archived INTEGER NOT NULL DEFAULT 0
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_ws_umo ON workspaces(umo, owner_user_id, archived)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_ws_cid ON workspaces(conversation_id)",
            """
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                id_prefix TEXT NOT NULL,
                workspace_id TEXT NOT NULL,
                executor TEXT NOT NULL,
                request_id TEXT NOT NULL UNIQUE,
                remote_task_id TEXT NOT NULL DEFAULT '',
                input_snapshot TEXT NOT NULL,
                status TEXT NOT NULL,
                result_ref TEXT NOT NULL DEFAULT '',
                artifact_refs TEXT NOT NULL DEFAULT '[]',
                error TEXT NOT NULL DEFAULT '',
                connectivity_error TEXT NOT NULL DEFAULT '',
                last_seq INTEGER NOT NULL DEFAULT 0,
                created_ts REAL NOT NULL,
                updated_ts REAL NOT NULL,
                started_ts REAL NOT NULL DEFAULT 0,
                ended_ts REAL NOT NULL DEFAULT 0,
                submit_attempts INTEGER NOT NULL DEFAULT 0,
                result_surfaced INTEGER NOT NULL DEFAULT 0
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_run_ws ON runs(workspace_id, created_ts)",
            "CREATE INDEX IF NOT EXISTS idx_run_status ON runs(status)",
            "CREATE INDEX IF NOT EXISTS idx_run_remote ON runs(remote_task_id)",
            """
            CREATE TABLE IF NOT EXISTS pending_requirements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id TEXT NOT NULL,
                run_id TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL,
                consumed INTEGER NOT NULL DEFAULT 0,
                created_ts REAL NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_pending_ws ON pending_requirements(workspace_id, consumed)",
            """
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                session TEXT NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                created_ts REAL NOT NULL,
                sent_ts REAL NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT ''
            )
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_key ON outbox(run_id, event_key)",
            "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, created_ts)",
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS quarantined_tasks (
                request_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_ts REAL NOT NULL
            )
            """,
        ],
    ),
    (
        2,
        [
            "ALTER TABLE runs ADD COLUMN cancel_intent INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE runs ADD COLUMN result_text TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE outbox ADD COLUMN next_attempt_at REAL NOT NULL DEFAULT 0",
            "ALTER TABLE outbox ADD COLUMN leased_until REAL NOT NULL DEFAULT 0",
            "CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(status, next_attempt_at)",
            (
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_run "
                "ON runs(workspace_id) WHERE status IN "
                "('queued','submitting','running','cancel_requested','needs_reconciliation')"
            ),
        ],
    ),
]

SCHEMA_VERSION = MIGRATIONS[-1][0]

_TERMINAL_PLACEHOLDERS = ",".join("?" for _ in TERMINAL)


class StoreClosedError(RuntimeError):
    pass


class Store:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._closed = False

    def _connect(self) -> sqlite3.Connection:
        if self._closed:
            raise StoreClosedError("store is closed")
        if self._conn is not None:
            return self._conn
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        self._conn = conn
        return conn

    def _migrate_sync(self) -> int:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations "
                    "(version INTEGER PRIMARY KEY, applied_ts REAL NOT NULL)"
                )
                current = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
                ).fetchone()[0]
                for version, statements in MIGRATIONS:
                    if version <= current:
                        continue
                    for stmt in statements:
                        conn.execute(stmt)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_ts) VALUES (?, ?)",
                        (version, time.time()),
                    )
                    current = version
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return current

    async def migrate(self) -> int:
        return await asyncio.to_thread(self._migrate_sync)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ------------------------------------------------------------------ ids
    def _prefix_free_sync(self, table: str, prefix: str) -> bool:
        conn = self._connect()
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE id_prefix = ? LIMIT 1", (prefix,)
        ).fetchone()
        return row is None

    async def prefix_free(self, table: str, prefix: str) -> bool:
        return await asyncio.to_thread(self._prefix_free_sync, table, prefix)

    # ------------------------------------------------------------ workspaces
    def _insert_workspace_sync(self, ws: Workspace) -> None:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO workspaces(id, id_prefix, owner_user_id, umo, "
                    "conversation_id, title, persona, memory_policy, cwd, "
                    "created_ts, updated_ts, archived) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        ws.id, ws.id[:9], ws.owner_user_id, ws.umo,
                        ws.conversation_id, ws.title, ws.persona, ws.memory_policy,
                        ws.cwd, ws.created_ts, ws.updated_ts, ws.archived,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def insert_workspace(self, ws: Workspace) -> None:
        await asyncio.to_thread(self._insert_workspace_sync, ws)

    @staticmethod
    def _ws_from_row(row) -> Workspace:
        return Workspace(
            id=row["id"], owner_user_id=row["owner_user_id"], umo=row["umo"],
            conversation_id=row["conversation_id"], title=row["title"],
            persona=row["persona"], memory_policy=row["memory_policy"],
            cwd=row["cwd"], created_ts=row["created_ts"], updated_ts=row["updated_ts"],
            archived=row["archived"],
        )

    def _get_workspace_sync(self, ws_id: str) -> Workspace | None:
        conn = self._connect()
        with self._lock:
            row = conn.execute("SELECT * FROM workspaces WHERE id = ?", (ws_id,)).fetchone()
        return self._ws_from_row(row) if row else None

    async def get_workspace(self, ws_id: str) -> Workspace | None:
        return await asyncio.to_thread(self._get_workspace_sync, ws_id)

    def _get_workspace_by_cid_sync(self, cid: str) -> Workspace | None:
        conn = self._connect()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM workspaces WHERE conversation_id = ?", (cid,)
            ).fetchone()
        return self._ws_from_row(row) if row else None

    async def get_workspace_by_cid(self, cid: str) -> Workspace | None:
        if not cid:
            return None
        return await asyncio.to_thread(self._get_workspace_by_cid_sync, cid)

    def _list_workspaces_sync(self, umo: str, owner: str | None) -> list[Workspace]:
        conn = self._connect()
        with self._lock:
            if owner is None:
                rows = conn.execute(
                    "SELECT * FROM workspaces WHERE umo = ? ORDER BY updated_ts DESC",
                    (umo,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM workspaces WHERE umo = ? AND owner_user_id = ? "
                    "ORDER BY updated_ts DESC",
                    (umo, owner),
                ).fetchall()
        return [self._ws_from_row(r) for r in rows]

    async def list_workspaces(self, umo: str, owner: str | None = None) -> list[Workspace]:
        return await asyncio.to_thread(self._list_workspaces_sync, umo, owner)

    def _all_workspaces_sync(self) -> list[Workspace]:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM workspaces ORDER BY updated_ts DESC"
            ).fetchall()
        return [self._ws_from_row(r) for r in rows]

    async def all_workspaces(self) -> list[Workspace]:
        return await asyncio.to_thread(self._all_workspaces_sync)

    def _update_workspace_sync(self, ws_id: str, fields: dict) -> None:
        if not fields:
            return
        conn = self._connect()
        cols = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [time.time(), ws_id]
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(f"UPDATE workspaces SET {cols}, updated_ts = ? WHERE id = ?", vals)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def update_workspace(self, ws_id: str, **fields) -> None:
        await asyncio.to_thread(self._update_workspace_sync, ws_id, fields)

    # ------------------------------------------------------------------ runs
    @staticmethod
    def _run_values(run: Run) -> tuple:
        return (
            run.id, run.id[:11], run.workspace_id, run.executor, run.request_id,
            run.remote_task_id, run.input_snapshot, run.status, run.result_ref,
            run.artifact_refs, run.error, run.connectivity_error, run.last_seq,
            run.created_ts, run.updated_ts, run.started_ts, run.ended_ts,
            run.submit_attempts, run.result_surfaced, run.cancel_intent, run.result_text,
        )

    def _reserve_run_sync(self, run: Run, pending_ids: list[int]) -> None:
        """Atomically reserve a workspace slot, insert the run, consume amendments."""
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                try:
                    conn.execute(
                        "INSERT INTO runs(id, id_prefix, workspace_id, executor, "
                        "request_id, remote_task_id, input_snapshot, status, result_ref, "
                        "artifact_refs, error, connectivity_error, last_seq, created_ts, "
                        "updated_ts, started_ts, ended_ts, submit_attempts, "
                        "result_surfaced, cancel_intent, result_text) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        self._run_values(run),
                    )
                except sqlite3.IntegrityError as e:
                    conn.execute("ROLLBACK")
                    raise ValueError("workspace_busy") from e
                if pending_ids:
                    placeholders = ",".join("?" for _ in pending_ids)
                    conn.execute(
                        f"UPDATE pending_requirements SET consumed = 1, run_id = ? "
                        f"WHERE id IN ({placeholders})",
                        (run.id, *pending_ids),
                    )
                conn.execute("COMMIT")
            except ValueError:
                raise
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def reserve_run(self, run: Run, pending_ids: list[int]) -> None:
        await asyncio.to_thread(self._reserve_run_sync, run, pending_ids)

    async def insert_run(self, run: Run) -> None:
        await asyncio.to_thread(self._insert_run_sync, run)

    def _insert_run_sync(self, run: Run) -> None:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO runs(id, id_prefix, workspace_id, executor, "
                    "request_id, remote_task_id, input_snapshot, status, result_ref, "
                    "artifact_refs, error, connectivity_error, last_seq, created_ts, "
                    "updated_ts, started_ts, ended_ts, submit_attempts, "
                    "result_surfaced, cancel_intent, result_text) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    self._run_values(run),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _run_from_row(row) -> Run:
        keys = row.keys()
        return Run(
            id=row["id"], workspace_id=row["workspace_id"], executor=row["executor"],
            request_id=row["request_id"], remote_task_id=row["remote_task_id"],
            input_snapshot=row["input_snapshot"], status=row["status"],
            result_ref=row["result_ref"], artifact_refs=row["artifact_refs"],
            error=row["error"], connectivity_error=row["connectivity_error"],
            last_seq=row["last_seq"], created_ts=row["created_ts"],
            updated_ts=row["updated_ts"], started_ts=row["started_ts"],
            ended_ts=row["ended_ts"], submit_attempts=row["submit_attempts"],
            result_surfaced=row["result_surfaced"],
            cancel_intent=row["cancel_intent"] if "cancel_intent" in keys else 0,
            result_text=row["result_text"] if "result_text" in keys else "",
        )

    def _get_run_sync(self, run_id: str) -> Run | None:
        conn = self._connect()
        with self._lock:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return self._run_from_row(row) if row else None

    async def get_run(self, run_id: str) -> Run | None:
        return await asyncio.to_thread(self._get_run_sync, run_id)

    def _get_run_by_request_sync(self, request_id: str) -> Run | None:
        conn = self._connect()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM runs WHERE request_id = ?", (request_id,)
            ).fetchone()
        return self._run_from_row(row) if row else None

    async def get_run_by_request(self, request_id: str) -> Run | None:
        return await asyncio.to_thread(self._get_run_by_request_sync, request_id)

    def _list_runs_sync(self, workspace_id: str, limit: int = 100) -> list[Run]:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM runs WHERE workspace_id = ? "
                "ORDER BY created_ts DESC LIMIT ?",
                (workspace_id, limit),
            ).fetchall()
        return [self._run_from_row(r) for r in rows]

    async def list_runs(self, workspace_id: str, limit: int = 100) -> list[Run]:
        return await asyncio.to_thread(self._list_runs_sync, workspace_id, limit)

    def _active_runs_sync(self) -> list[Run]:
        conn = self._connect()
        placeholders = ",".join("?" for _ in ACTIVE)
        with self._lock:
            rows = conn.execute(
                f"SELECT * FROM runs WHERE status IN ({placeholders}) "
                "ORDER BY created_ts ASC",
                tuple(ACTIVE),
            ).fetchall()
        return [self._run_from_row(r) for r in rows]

    async def active_runs(self) -> list[Run]:
        return await asyncio.to_thread(self._active_runs_sync)

    def _active_runs_for_ws_sync(self, workspace_id: str) -> list[Run]:
        conn = self._connect()
        placeholders = ",".join("?" for _ in ACTIVE)
        with self._lock:
            rows = conn.execute(
                f"SELECT * FROM runs WHERE workspace_id = ? AND status IN ({placeholders})",
                (workspace_id, *ACTIVE),
            ).fetchall()
        return [self._run_from_row(r) for r in rows]

    async def active_runs_for_workspace(self, workspace_id: str) -> list[Run]:
        return await asyncio.to_thread(self._active_runs_for_ws_sync, workspace_id)

    def _workspace_has_capacity_sync(self, workspace_id: str) -> bool:
        return not self._active_runs_for_ws_sync(workspace_id)

    async def workspace_has_capacity(self, workspace_id: str) -> bool:
        return await asyncio.to_thread(self._workspace_has_capacity_sync, workspace_id)

    def _update_run_sync(self, run_id: str, fields: dict, allow_terminal: bool) -> bool:
        if not fields:
            return False
        conn = self._connect()
        cols = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [time.time(), run_id]
        guard = "" if allow_terminal else f" AND status NOT IN ({_TERMINAL_PLACEHOLDERS})"
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    f"UPDATE runs SET {cols}, updated_ts = ? WHERE id = ?{guard}",
                    vals + ([] if allow_terminal else list(TERMINAL)),
                )
                conn.execute("COMMIT")
                return cur.rowcount > 0
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def update_run(self, run_id: str, allow_terminal: bool = False, **fields) -> bool:
        return await asyncio.to_thread(self._update_run_sync, run_id, fields, allow_terminal)

    def _attach_remote_sync(self, run_id: str, task_id: str) -> None:
        """Attach a remote task id without clearing cancel intent or status."""
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE runs SET remote_task_id = ?, "
                    "status = CASE WHEN cancel_intent = 1 THEN 'cancel_requested' "
                    "ELSE 'running' END, started_ts = COALESCE(NULLIF(started_ts,0), ?), "
                    "updated_ts = ? WHERE id = ? AND status NOT IN "
                    f"({_TERMINAL_PLACEHOLDERS})",
                    (task_id, time.time(), time.time(), run_id, *TERMINAL),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def attach_remote_task(self, run_id: str, task_id: str) -> None:
        await asyncio.to_thread(self._attach_remote_sync, run_id, task_id)

    def _claim_for_submit_sync(self, run_id: str, started_ts: float) -> bool:
        """Atomically move queued -> submitting exactly once."""
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE runs SET status = 'submitting', "
                    "submit_attempts = submit_attempts + 1, started_ts = ?, "
                    "updated_ts = ? WHERE id = ? AND status = 'queued'",
                    (started_ts, time.time(), run_id),
                )
                conn.execute("COMMIT")
                return cur.rowcount > 0
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def claim_for_submit(self, run_id: str, started_ts: float) -> bool:
        return await asyncio.to_thread(self._claim_for_submit_sync, run_id, started_ts)

    def _finalize_run_sync(self, run_id: str, status: str, result_text: str,
                           artifact_refs: str, outbox_items: list[OutboxItem],
                           error: str = "", connectivity_error: str | None = None) -> bool:
        """Atomically set terminal state, result text and enqueue notifications."""
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                sets = [("status", status), ("result_text", result_text),
                        ("artifact_refs", artifact_refs), ("ended_ts", time.time()),
                        ("updated_ts", time.time())]
                if error:
                    sets.append(("error", error))
                if connectivity_error is not None:
                    sets.append(("connectivity_error", connectivity_error))
                cols = ", ".join(f"{k} = ?" for k, _ in sets)
                vals = [v for _, v in sets]
                cur = conn.execute(
                    f"UPDATE runs SET {cols} WHERE id = ? AND status NOT IN "
                    f"({_TERMINAL_PLACEHOLDERS})",
                    vals + [run_id, *TERMINAL],
                )
                if cur.rowcount == 0:
                    conn.execute("ROLLBACK")
                    return False
                for item in outbox_items:
                    conn.execute(
                        "INSERT OR IGNORE INTO outbox(run_id, event_key, session, "
                        "content, status, attempts, created_ts, sent_ts, last_error, "
                        "next_attempt_at, leased_until) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (item.run_id, item.event_key, item.session, item.content,
                         "pending", 0, item.created_ts or time.time(), 0.0, "",
                         time.time(), 0.0),
                    )
                conn.execute("COMMIT")
                return True
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def finalize_run(self, run_id: str, status: str, result_text: str,
                           artifact_refs: str, outbox_items: list[OutboxItem],
                           error: str = "", connectivity_error: str | None = None) -> bool:
        return await asyncio.to_thread(
            self._finalize_run_sync, run_id, status, result_text, artifact_refs,
            outbox_items, error, connectivity_error,
        )

    # ---------------------------------------------------- pending requirements
    def _add_pending_sync(self, workspace_id: str, text: str) -> int:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "INSERT INTO pending_requirements(workspace_id, run_id, text, "
                    "consumed, created_ts) VALUES (?,?,?,0,?)",
                    (workspace_id, "", text, time.time()),
                )
                conn.execute("COMMIT")
                return int(cur.lastrowid)
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def add_pending(self, workspace_id: str, text: str) -> int:
        return await asyncio.to_thread(self._add_pending_sync, workspace_id, text)

    def _list_unconsumed_sync(self, workspace_id: str) -> list[PendingRequirement]:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM pending_requirements WHERE workspace_id = ? "
                "AND consumed = 0 ORDER BY created_ts ASC",
                (workspace_id,),
            ).fetchall()
        return [
            PendingRequirement(
                id=r["id"], workspace_id=r["workspace_id"], text=r["text"],
                run_id=r["run_id"], consumed=r["consumed"], created_ts=r["created_ts"],
            )
            for r in rows
        ]

    async def pending_unconsumed(self, workspace_id: str) -> list[PendingRequirement]:
        return await asyncio.to_thread(self._list_unconsumed_sync, workspace_id)

    def _consume_pending_sync(self, ids: list[int], run_id: str) -> None:
        if not ids:
            return
        conn = self._connect()
        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    f"UPDATE pending_requirements SET consumed = 1, run_id = ? "
                    f"WHERE id IN ({placeholders})",
                    (run_id, *ids),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def consume_pending(self, ids: list[int], run_id: str) -> None:
        await asyncio.to_thread(self._consume_pending_sync, ids, run_id)

    # ---------------------------------------------------------------- outbox
    def _enqueue_outbox_sync(self, item: OutboxItem) -> bool:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO outbox(run_id, event_key, session, content, "
                    "status, attempts, created_ts, sent_ts, last_error, next_attempt_at, "
                    "leased_until) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (item.run_id, item.event_key, item.session, item.content,
                     "pending", 0, item.created_ts or time.time(), 0.0, "",
                     time.time(), 0.0),
                )
                conn.execute("COMMIT")
                return cur.rowcount > 0
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def enqueue_outbox(self, item: OutboxItem) -> bool:
        return await asyncio.to_thread(self._enqueue_outbox_sync, item)

    def _claim_outbox_sync(self, limit: int, lease_s: float) -> list[OutboxItem]:
        now = time.time()
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT * FROM outbox WHERE status = 'pending' "
                    "AND next_attempt_at <= ? AND leased_until <= ? "
                    "ORDER BY next_attempt_at ASC, created_ts ASC LIMIT ?",
                    (now, now, limit),
                ).fetchall()
                ids = [r["id"] for r in rows]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    conn.execute(
                        f"UPDATE outbox SET leased_until = ? WHERE id IN ({placeholders})",
                        (now + lease_s, *ids),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return [self._outbox_from_row(r) for r in rows]

    async def claim_outbox(self, limit: int = 20, lease_s: float = 120.0) -> list[OutboxItem]:
        return await asyncio.to_thread(self._claim_outbox_sync, limit, lease_s)

    @staticmethod
    def _outbox_from_row(r) -> OutboxItem:
        keys = r.keys()
        return OutboxItem(
            id=r["id"], run_id=r["run_id"], event_key=r["event_key"],
            session=r["session"], content=r["content"], status=r["status"],
            attempts=r["attempts"], created_ts=r["created_ts"], sent_ts=r["sent_ts"],
            last_error=r["last_error"],
            next_attempt_at=r["next_attempt_at"] if "next_attempt_at" in keys else 0.0,
            leased_until=r["leased_until"] if "leased_until" in keys else 0.0,
        )

    # Backwards-compatible read-only view for tests/diagnostics.
    def _due_outbox_sync(self, limit: int) -> list[OutboxItem]:
        conn = self._connect()
        now = time.time()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE status = 'pending' "
                "AND next_attempt_at <= ? AND leased_until <= ? "
                "ORDER BY next_attempt_at ASC, created_ts ASC LIMIT ?",
                (now, now, limit),
            ).fetchall()
        return [self._outbox_from_row(r) for r in rows]

    async def due_outbox(self, limit: int = 20) -> list[OutboxItem]:
        return await asyncio.to_thread(self._due_outbox_sync, limit)

    def _mark_outbox_sent_sync(self, item_id: int) -> None:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE outbox SET status='sent', sent_ts=?, last_error='', "
                    "leased_until=0 WHERE id=?",
                    (time.time(), item_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def mark_outbox_sent(self, item_id: int) -> None:
        await asyncio.to_thread(self._mark_outbox_sent_sync, item_id)

    def _mark_outbox_failed_sync(self, item_id: int, error: str, attempts: int) -> None:
        backoff = _OUTBOX_BACKOFF[min(attempts, len(_OUTBOX_BACKOFF) - 1)]
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE outbox SET status='pending', attempts=?, last_error=?, "
                    "next_attempt_at=?, leased_until=0 WHERE id=?",
                    (attempts, error[:500], time.time() + backoff, item_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def mark_outbox_failed(self, item_id: int, error: str, attempts: int = 1) -> None:
        await asyncio.to_thread(self._mark_outbox_failed_sync, item_id, error, attempts)

    # ----------------------------------------------------------- quarantine
    def _quarantine_sync(self, request_id: str, payload: dict, reason: str) -> None:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO quarantined_tasks(request_id, payload, "
                    "reason, created_ts) VALUES (?,?,?,?)",
                    (request_id, json.dumps(payload, ensure_ascii=False), reason, time.time()),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def quarantine(self, request_id: str, payload: dict, reason: str) -> None:
        await asyncio.to_thread(self._quarantine_sync, request_id, payload, reason)

    def _list_quarantine_sync(self) -> list[dict]:
        conn = self._connect()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM quarantined_tasks ORDER BY created_ts DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    async def list_quarantine(self) -> list[dict]:
        return await asyncio.to_thread(self._list_quarantine_sync)

    def _meta_get_sync(self, key: str, default=None):
        conn = self._connect()
        with self._lock:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    async def meta_get(self, key: str, default=None):
        return await asyncio.to_thread(self._meta_get_sync, key, default)

    def _meta_set_sync(self, key: str, value: str) -> None:
        conn = self._connect()
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    async def meta_set(self, key: str, value: str) -> None:
        await asyncio.to_thread(self._meta_set_sync, key, value)
