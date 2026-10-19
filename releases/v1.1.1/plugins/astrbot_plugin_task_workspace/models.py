# -*- coding: utf-8 -*-
"""Data model for the task workspace plugin.

Workspaces are persistent conversations. Runs are single executions of an
executor (dsh/codex) bound to a workspace. Connectivity failures are tracked
separately from execution state so an offline runner never masquerades as a
failed task.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from enum import Enum


class RunStatus(str, Enum):
    QUEUED = "queued"
    SUBMITTING = "submitting"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    NEEDS_RECONCILIATION = "needs_reconciliation"


TERMINAL: frozenset[str] = frozenset(
    {
        RunStatus.SUCCEEDED.value,
        RunStatus.FAILED.value,
        RunStatus.CANCELLED.value,
        RunStatus.TIMED_OUT.value,
    }
)

# ``needs_reconciliation`` is NOT terminal: it must retain capacity and be
# recovered on startup until reconciliation authoritatively resolves absence.
ACTIVE: frozenset[str] = frozenset(
    {
        RunStatus.QUEUED.value,
        RunStatus.SUBMITTING.value,
        RunStatus.RUNNING.value,
        RunStatus.CANCEL_REQUESTED.value,
        RunStatus.NEEDS_RECONCILIATION.value,
    }
)

# Statuses that occupy a workspace slot (used for the partial unique index).
ACTIVE_SQL = tuple(sorted(ACTIVE))

EXECUTORS = ("dsh", "codex")


def now() -> float:
    return time.time()


@dataclass
class Workspace:
    id: str
    owner_user_id: str
    umo: str
    conversation_id: str
    title: str
    persona: str = ""
    memory_policy: str = "isolated"
    cwd: str = ""
    created_ts: float = field(default_factory=now)
    updated_ts: float = field(default_factory=now)
    archived: int = 0
    environment: str = "windows_remote"

    @property
    def short_id(self) -> str:
        return self.id[:9]

    def to_row(self) -> dict:
        return asdict(self)


@dataclass
class Run:
    id: str
    workspace_id: str
    executor: str
    request_id: str
    input_snapshot: str
    remote_task_id: str = ""
    status: str = RunStatus.QUEUED.value
    result_ref: str = ""
    artifact_refs: str = "[]"
    error: str = ""
    connectivity_error: str = ""
    last_seq: int = 0
    created_ts: float = field(default_factory=now)
    updated_ts: float = field(default_factory=now)
    started_ts: float = 0.0
    ended_ts: float = 0.0
    submit_attempts: int = 0
    result_surfaced: int = 0
    cancel_intent: int = 0
    result_text: str = ""

    @property
    def short_id(self) -> str:
        return self.id[:11]

    def is_terminal(self) -> bool:
        return self.status in TERMINAL

    def to_row(self) -> dict:
        return asdict(self)


@dataclass
class PendingRequirement:
    id: int | None
    workspace_id: str
    text: str
    run_id: str = ""
    consumed: int = 0
    created_ts: float = field(default_factory=now)


@dataclass
class OutboxItem:
    id: int | None
    run_id: str
    event_key: str
    session: str
    content: str
    status: str = "pending"
    attempts: int = 0
    created_ts: float = field(default_factory=now)
    sent_ts: float = 0.0
    last_error: str = ""
    next_attempt_at: float = 0.0
    leased_until: float = 0.0
