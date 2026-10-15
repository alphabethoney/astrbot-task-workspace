# -*- coding: utf-8 -*-
"""任务表：维护服务端视角下的任务状态机与增量游标，并持久化到 JSON。

重启后自动加载，避免重复播报与游标丢失；每次变更即时落盘。
"""
import json
import os
import time
from dataclasses import dataclass, field, asdict

# 还在跑、需要继续轮询的状态
ACTIVE = {"queued", "running"}
# 终态：不再轮询
TERMINAL = {"succeeded", "failed", "timed_out", "cancelled"}


@dataclass
class TaskRecord:
    task_id: str
    agent: str = ""
    prompt: str = ""
    request_id: str = ""       # 幂等请求 ID（远端去重与本地响应丢失恢复）
    session: str = ""          # 触发该任务的聊天会话（unified_msg_origin）
    conversation_id: str = ""  # 归属的任务对话（conversation_id）
    status: str = "queued"     # 服务端已知的远端状态
    last_seq: int = 0          # 最后读到的远端事件 seq（增量游标）
    created: float = field(default_factory=time.time)
    last_poll: float = 0.0     # 最近一次 watch 时间
    last_announce: float = 0.0  # 最近一次播报时间
    quarantined: bool = False   # 隔离行：不轮询、不自动播报
    quarantine_reason: str = ""
    quarantine_evidence: dict = field(default_factory=dict)
    quarantine_original: dict = field(default_factory=dict)

    @property
    def is_terminal(self):
        return self.status in TERMINAL


# Poll ownership marker persisted in the task table JSON.
POLLER_LEGACY = "legacy"
POLLER_TASK_WORKSPACE = "task_workspace"

HANDOFF_VERSION = 1


class HandoffError(RuntimeError):
    """Raised when a handoff marker cannot be durably persisted/read back."""


class TaskTable:
    def __init__(self, path=None):
        self._tasks = {}          # task_id -> TaskRecord
        self._session_tasks = {}  # session -> [task_id...]，最新在前
        self._conversation_tasks = {}  # conversation_id -> [task_id...]，最新在前
        self._request_tasks = {}  # request_id -> task_id（幂等）
        if path is None:
            path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "task_table.json"
            )
        self._path = path
        self._poller_owner = POLLER_LEGACY
        self._handoff = {}
        self.load()

    # ---------------------------------------------------------------- 持久化
    def _snapshot(self):
        return {
            "tasks": {tid: asdict(r) for tid, r in self._tasks.items()},
            "session_tasks": self._session_tasks,
            "conversation_tasks": self._conversation_tasks,
            "request_tasks": self._request_tasks,
            "poller_owner": self._poller_owner,
            "handoff": self._handoff,
        }

    def _write(self):
        """Write atomically; raise on failure (never silently swallow)."""
        data = self._snapshot()
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, self._path)

    def _save(self):
        try:
            self._write()
        except Exception:
            # Best-effort for high-frequency scheduling writes.
            pass

    def _save_checked(self, *, verify):
        """Write and verify by re-reading from disk; raise HandoffError."""
        try:
            self._write()
        except Exception as e:
            raise HandoffError(f"handoff persist failed: {e}") from e
        try:
            with open(self._path, encoding="utf-8") as f:
                disk = json.load(f)
        except Exception as e:
            raise HandoffError(f"handoff readback failed: {e}") from e
        if not verify(disk):
            raise HandoffError("handoff readback mismatch")

    def save(self):
        """显式落盘（供调用方在直接改字段后调用）。"""
        self._save()

    def load(self):
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
            self._tasks = {
                tid: TaskRecord(**d) for tid, d in data.get("tasks", {}).items()
            }
            self._session_tasks = {
                k: list(v) for k, v in data.get("session_tasks", {}).items()
            }
            self._conversation_tasks = {
                k: list(v) for k, v in data.get("conversation_tasks", {}).items()
            }
            self._request_tasks = {
                k: v for k, v in data.get("request_tasks", {}).items()
            }
            for tid, rec in self._tasks.items():
                if rec.request_id:
                    self._request_tasks.setdefault(rec.request_id, tid)
            self._poller_owner = data.get("poller_owner", POLLER_LEGACY)
            self._handoff = data.get("handoff", {}) or {}
        except Exception:
            self._tasks = {}
            self._session_tasks = {}
            self._conversation_tasks = {}
            self._request_tasks = {}
            self._poller_owner = POLLER_LEGACY
            self._handoff = {}

    # ---------------------------------------------------------- 轮询所有权
    def poller_owner(self):
        return self._poller_owner

    def set_poller_owner(self, owner):
        """Record the single poll owner, verifying the persisted readback."""
        if owner not in (POLLER_LEGACY, POLLER_TASK_WORKSPACE):
            raise ValueError(f"unknown poller owner: {owner}")
        previous = self._poller_owner
        self._poller_owner = owner
        try:
            self._save_checked(verify=lambda d: d.get("poller_owner") == owner)
        except HandoffError:
            self._poller_owner = previous
            raise

    def record_handoff(self, info):
        """Persist a handoff marker and verify the readback.

        The marker embeds the poller owner and a version so a later read can
        prove the handoff actually committed (``TaskTable._save`` used to
        swallow errors and could silently "succeed").
        """
        marker = dict(info or {})
        marker["version"] = HANDOFF_VERSION
        marker["poller_owner"] = POLLER_TASK_WORKSPACE
        marker.setdefault("ts", time.time())
        previous_handoff = self._handoff
        previous_owner = self._poller_owner
        self._handoff = marker
        self._poller_owner = POLLER_TASK_WORKSPACE
        marker_ts = marker.get("ts")
        try:
            self._save_checked(
                verify=lambda d: (
                    (d.get("handoff") or {}).get("version") == HANDOFF_VERSION
                    and (d.get("handoff") or {}).get("ts") == marker_ts
                    and d.get("poller_owner") == POLLER_TASK_WORKSPACE
                )
            )
        except HandoffError:
            self._handoff = previous_handoff
            self._poller_owner = previous_owner
            raise
        return marker

    def handoff(self):
        return dict(self._handoff or {})

    def handoff_ok(self):
        """True only when a persisted, readback-verified handoff exists."""
        try:
            with open(self._path, encoding="utf-8") as f:
                disk = json.load(f)
        except Exception:
            return False
        marker = disk.get("handoff") or {}
        return (
            disk.get("poller_owner") == POLLER_TASK_WORKSPACE
            and marker.get("version") == HANDOFF_VERSION
            and bool(marker)
        )

    def managed(self):
        return self._poller_owner == POLLER_TASK_WORKSPACE

    def quarantine(self, task_id, reason="", evidence=None):
        """Isolate a row so it is never polled or auto-broadcast again.

        The original record is preserved (``quarantine_original``) and the row is
        never deleted or rewritten to a terminal status, so historical evidence
        survives and the row is not silently resubmitted.
        """
        rec = self._tasks.get(task_id)
        if rec is not None:
            rec.quarantined = True
            rec.quarantine_reason = reason
            rec.quarantine_evidence = dict(evidence or {})
            rec.quarantine_original = asdict(rec)
            # asdict above already includes the quarantined flags; keep a clean
            # pre-quarantine snapshot for evidence.
            rec.quarantine_original["quarantined"] = False
            rec.quarantine_original["quarantine_reason"] = ""
            rec.quarantine_original["quarantine_evidence"] = {}
            rec.quarantine_original["quarantine_original"] = {}
            self._save()
            return True
        return False

    # ---------------------------------------------------------------- 增改查
    def add(self, task_id, agent, prompt, session, conversation_id="",
            request_id="") -> TaskRecord:
        rec = TaskRecord(task_id=task_id, agent=agent, prompt=prompt,
                         request_id=request_id, session=session,
                         conversation_id=conversation_id)
        self._tasks[task_id] = rec
        if request_id:
            self._request_tasks[request_id] = task_id
        self._session_tasks.setdefault(session, []).insert(0, task_id)
        self._conversation_tasks.setdefault(conversation_id, []).insert(0, task_id)
        self._save()
        return rec

    def get_by_request(self, request_id):
        """按幂等请求 ID 查任务，用于重复提交与响应丢失恢复。"""
        if not request_id:
            return None
        tid = self._request_tasks.get(request_id)
        return self._tasks.get(tid) if tid else None

    def get(self, task_id):
        return self._tasks.get(task_id)

    def all(self):
        return list(self._tasks.values())

    def active(self):
        """仍在跑、需要轮询的任务（隔离行除外）。"""
        return [r for r in self._tasks.values()
                if not r.is_terminal and not r.quarantined]

    def set_status(self, task_id, status):
        rec = self._tasks.get(task_id)
        if rec is not None:
            rec.status = status
            self._save()

    def bump_seq(self, task_id, seq):
        rec = self._tasks.get(task_id)
        if rec is not None and seq > rec.last_seq:
            rec.last_seq = seq
            self._save()

    def recent_for(self, session):
        """某会话最近提交的一个任务。"""
        for tid in self._session_tasks.get(session, []):
            rec = self._tasks.get(tid)
            if rec is not None:
                return rec
        return None

    def recent_for_conversation(self, conversation_id):
        """某任务对话最近提交的一个任务。"""
        for tid in self._conversation_tasks.get(conversation_id, []):
            rec = self._tasks.get(tid)
            if rec is not None:
                return rec
        return None

    def conversation_tasks(self, conversation_id):
        """某任务对话的全部任务（最新在前）。"""
        return [
            self._tasks[t]
            for t in self._conversation_tasks.get(conversation_id, [])
            if t in self._tasks
        ]

    def recent(self):
        """全局最近提交的一个任务。"""
        if not self._tasks:
            return None
        return max(self._tasks.values(), key=lambda r: r.created)
