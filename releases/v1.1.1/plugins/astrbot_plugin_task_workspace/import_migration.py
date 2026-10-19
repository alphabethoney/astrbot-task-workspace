# -*- coding: utf-8 -*-
"""Dry-run import of the legacy bridge task table.

Ownership is derived from the verified conversation (``conv.user_id`` must equal
the record UMO) and the UMO's session id, never from record-supplied values.
Apply revalidates every record; malformed/unresolved records are quarantined
individually. Active legacy jobs are adopted by remote task id; only one active
adoption per workspace is allowed (the newest), older ones are quarantined as
duplicates so two pollers never own the same run.
"""
from __future__ import annotations

import json
import secrets
from dataclasses import dataclass, field
from pathlib import Path

from .models import Run, RunStatus, Workspace, now


@dataclass
class ImportReport:
    total: int = 0
    importable: list[dict] = field(default_factory=list)
    quarantined: list[dict] = field(default_factory=list)
    adopted_active: int = 0
    imported: int = 0

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "importable": len(self.importable),
            "quarantined": len(self.quarantined),
            "adopted_active": self.adopted_active,
            "imported": self.imported,
            "quarantine_details": self.quarantined,
        }


LEGACY_STATUS_MAP = {
    "queued": RunStatus.QUEUED.value,
    "running": RunStatus.RUNNING.value,
    "succeeded": RunStatus.SUCCEEDED.value,
    "failed": RunStatus.FAILED.value,
    "timed_out": RunStatus.TIMED_OUT.value,
    "cancelled": RunStatus.CANCELLED.value,
    "cancel_requested": RunStatus.CANCEL_REQUESTED.value,
}
ACTIVE_LEGACY = {"queued", "running", "cancel_requested"}


def _owner_from_umo(umo: str) -> str:
    parts = str(umo or "").split(":")
    return parts[-1].strip() if parts else ""


# Message-type tokens that denote a verified *private* conversation whose last
# UMO segment is the owning user id. Production uses ``FriendMessage`` (e.g.
# ``alphabet:FriendMessage:10001``); ``private`` is the generic form.
_PRIVATE_TYPE_TOKENS = frozenset({"private", "friendmessage"})
_GROUP_TYPE_TOKENS = frozenset({"group", "guild", "groupmessage", "guildmessage"})


def _umo_kind(umo: str) -> str:
    """Classify a UMO as private / group / unknown.

    Only a verified private shape (``platform:<private|FriendMessage>:user``)
    yields an owner we may derive automatically. Group/unknown shapes require an
    explicit owner mapping or they are quarantined rather than guessed. We never
    invent an owner literal from a group id.
    """
    parts = [p.strip() for p in str(umo or "").split(":")]
    if len(parts) >= 3 and parts[1].lower() in _PRIVATE_TYPE_TOKENS and parts[-1]:
        return "private"
    if len(parts) >= 3 and parts[1].lower() in _GROUP_TYPE_TOKENS:
        return "group"
    return "unknown"


class LegacyImporter:
    def __init__(self, store, cm, owner_map: dict | None = None,
                 selected_quarantine: dict | None = None):
        self.store = store
        self.cm = cm
        # Explicit owner assignment for group/unknown UMOs (or overrides).
        self.owner_map = dict(owner_map or {})
        # Explicit admin-selected historical quarantine: task_id -> info with
        # ``reason`` and optional ``evidence``. Only listed rows are excluded;
        # unknown active rows are never discarded globally.
        self.selected_quarantine = dict(selected_quarantine or {})

    def owner_for(self, umo: str) -> str:
        explicit = self.owner_map.get(umo)
        if explicit:
            return str(explicit).strip()
        if _umo_kind(umo) == "private":
            return _owner_from_umo(umo)
        return ""

    async def _verify(self, rec: dict) -> tuple[bool, str]:
        if not isinstance(rec, dict):
            return False, "malformed_record"
        if rec.get("quarantined"):
            return False, f"legacy_quarantined:{rec.get('quarantine_reason') or 'unspecified'}"
        cid = str(rec.get("conversation_id") or "")
        umo = str(rec.get("session") or "")
        if not cid:
            return False, "empty_conversation_id"
        if not umo:
            return False, "empty_umo"
        if not self.owner_for(umo):
            # Group/unknown UMO without an explicit verified owner mapping.
            return False, f"unsupported_umo_shape:{_umo_kind(umo)}"
        if self.cm is None:
            return False, "conversation_manager_unavailable"
        try:
            conv = await self.cm.get_conversation(umo, cid)
        except Exception as e:
            return False, f"lookup_error:{e}"
        if conv is None:
            return False, "conversation_not_found"
        conv_user = str(getattr(conv, "user_id", "") or "")
        if not conv_user:
            return False, "conversation_owner_missing"
        # The authoritative conversation owner MUST equal the UMO exactly; the
        # owner is never taken from the record-supplied value.
        if conv_user != umo:
            return False, "owner_mismatch"
        status = str(rec.get("status") or "")
        if status not in LEGACY_STATUS_MAP:
            return False, f"unknown_status:{status or 'empty'}"
        return True, ""

    async def dry_run(self, path: str | Path) -> ImportReport:
        report = ImportReport()
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except Exception as e:
            report.quarantined.append({"reason": f"unreadable:{e}"})
            return report
        tasks = data.get("tasks", {}) if isinstance(data, dict) else {}
        if not isinstance(tasks, dict):
            report.quarantined.append({"reason": "malformed_table"})
            return report
        report.total = len(tasks)
        active_by_cid: dict[str, dict] = {}
        for task_id, rec in tasks.items():
            if not isinstance(rec, dict):
                report.quarantined.append({"task_id": task_id, "reason": "malformed_record"})
                continue
            rec = dict(rec)
            rec.setdefault("task_id", task_id)
            selected = self.selected_quarantine.get(str(task_id))
            if selected is not None:
                report.quarantined.append({
                    "task_id": task_id,
                    "reason": str((selected or {}).get("reason") or "admin_selected_quarantine"),
                    "evidence": (selected or {}).get("evidence"),
                    "original": rec,
                })
                continue
            ok, reason = await self._verify(rec)
            if not ok:
                report.quarantined.append(
                    {"task_id": task_id, "reason": reason,
                     "conversation_id": rec.get("conversation_id", "")})
                continue
            if rec.get("status") in ACTIVE_LEGACY:
                cid = str(rec.get("conversation_id"))
                existing = active_by_cid.get(cid)
                if existing is not None:
                    older = min(existing, rec, key=lambda r: float(r.get("created") or 0))
                    newer = existing if older is rec else rec
                    report.quarantined.append(
                        {"task_id": older.get("task_id"),
                         "reason": "duplicate_active_for_conversation"})
                    active_by_cid[cid] = newer
                    report.importable = [
                        r for r in report.importable
                        if r.get("task_id") != older.get("task_id")
                    ]
                    if newer is rec:
                        report.importable.append(rec)
                else:
                    active_by_cid[cid] = rec
                    report.importable.append(rec)
                report.adopted_active += 1
            else:
                report.importable.append(rec)
        return report

    async def _alloc_ws_id(self, cid: str) -> str:
        for _ in range(20):
            candidate = f"ws_{secrets.token_hex(4)}"
            if await self.store.prefix_free("workspaces", candidate[:9]):
                return candidate
        raise RuntimeError("cannot allocate workspace id")

    async def apply(self, report: ImportReport) -> int:
        imported = 0
        valid = []
        for rec in list(report.importable):
            ok, reason = await self._verify(rec)
            if not ok:
                report.quarantined.append({"task_id": rec.get("task_id"), "reason": f"revalidate:{reason}"})
                continue
            valid.append(rec)
        report.importable = valid
        for rec in report.importable:
            cid = str(rec.get("conversation_id"))
            umo = str(rec.get("session"))
            owner = self.owner_for(umo)
            ws = await self.store.get_workspace_by_cid(cid)
            if ws is None:
                ws = Workspace(
                    id=await self._alloc_ws_id(cid), owner_user_id=owner, umo=umo,
                    conversation_id=cid, title=str(rec.get("prompt") or "legacy task")[:20],
                    persona="", memory_policy="legacy", cwd="",
                )
                await self.store.insert_workspace(ws)
            request_id = f"legacy:{rec.get('task_id')}"
            if await self.store.get_run_by_request(request_id):
                continue
            status = LEGACY_STATUS_MAP[str(rec.get("status"))]
            run = Run(
                id=f"run_{secrets.token_hex(4)}", workspace_id=ws.id,
                executor=str(rec.get("agent") or "dsh"), request_id=request_id,
                input_snapshot=json.dumps(
                    {"schema": 1, "prompt": rec.get("prompt", ""), "legacy": True}),
                remote_task_id=str(rec.get("task_id") or ""), status=status,
                last_seq=int(rec.get("last_seq") or 0),
                result_ref=str(rec.get("result_ref") or rec.get("result") or ""),
                error=str(rec.get("error") or ""),
                started_ts=float(rec.get("last_poll") or 0),
                created_ts=float(rec.get("created") or now()),
            )
            try:
                await self.store.insert_run(run)
                imported += 1
            except Exception as e:
                report.quarantined.append({"task_id": rec.get("task_id"),
                                           "reason": f"insert_failed:{e}"})
        report.imported = imported
        return imported

    async def quarantine_report(self, report: ImportReport) -> None:
        for item in report.quarantined:
            tid = str(item.get("task_id") or f"unknown-{item.get('reason')}")
            await self.store.quarantine(tid, item, item.get("reason", "unknown"))
