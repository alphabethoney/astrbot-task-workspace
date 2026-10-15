# -*- coding: utf-8 -*-
"""Offline, idempotent legacy-bridge -> task-workspace handoff helper.

This module is meant to be run by the parent *before* AstrBot startup (or called
from an admin script). It never touches the live plugin process and never sends
anything to QQ/SSH.

It takes an explicit, already-validated mapping (task id -> verified owner /
conversation) and:

1. refuses to hand off while any legacy row is still active (not drained);
2. records a versioned handoff marker with a readback verification so a
   swallowed JSON write cannot silently "succeed";
3. is idempotent: running it again with the same plan converges to the same
   persisted state.

Selected historical rows can be quarantined explicitly with evidence; unknown
active rows are never discarded globally.
"""
from __future__ import annotations

import json
from pathlib import Path

from .task_table import HANDOFF_VERSION, HandoffError, TaskTable


def build_handoff_plan(active_records, validated):
    """Split active rows into ``approved``/``rejected`` using a validated map.

    ``validated`` maps ``task_id`` -> ``{"owner": str, "conversation_id": str}``
    (or any truthy object proving the owner was verified). Rows without a
    verified mapping are rejected, never guessed.
    """
    approved, rejected = [], []
    for rec in active_records or []:
        tid = str((rec or {}).get("task_id") or "")
        info = validated.get(tid) if isinstance(validated, dict) else None
        if not info:
            rejected.append({"task_id": tid, "reason": "unmapped"})
            continue
        owner = ""
        if isinstance(info, dict):
            owner = str(info.get("owner") or "").strip()
        if not owner:
            rejected.append({"task_id": tid, "reason": "owner_missing"})
            continue
        approved.append(tid)
    return {"approved": approved, "rejected": rejected}


def write_handoff(table_path, plan=None, *, source="legacy_bridge",
                  drained=False, quarantine=None):
    """Persist the handoff marker (idempotent) and verify the readback.

    ``quarantine`` maps ``task_id`` -> ``{"reason": ..., "evidence": ...}`` for
    admin-selected rows; quarantine happens before the drain check so a selected
    stale row stops counting as active.
    """
    table = TaskTable(path=str(table_path))
    for tid, info in (quarantine or {}).items():
        info = info or {}
        table.quarantine(str(tid), str(info.get("reason") or "admin_selected"),
                         evidence=info.get("evidence"))
    # Active rows that are not explicitly validated for transactional adoption
    # must be drained first. Validated actives are handed to the workspace
    # poller exactly once (no double worker).
    approved = set()
    if isinstance(plan, dict):
        approved = {str(t) for t in (plan.get("approved") or [])}
    remaining = [r.task_id for r in table.active() if r.task_id not in approved]
    if remaining and not drained:
        raise HandoffError(
            "active legacy jobs remain; drain them or validate/quarantine "
            "explicitly before handoff: " + ", ".join(remaining))
    marker = table.record_handoff({
        "source": source,
        "plan": plan or {},
        "drained": bool(drained),
    })
    return marker


def handoff_ready(table_path) -> bool:
    return TaskTable(path=str(table_path)).handoff_ok()


def read_handoff(table_path) -> dict:
    table = TaskTable(path=str(table_path))
    return {"poller_owner": table.poller_owner(), "handoff": table.handoff(),
            "ready": table.handoff_ok()}


if __name__ == "__main__":  # pragma: no cover - explicit offline utility
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("table", help="path to the legacy task_table.json")
    ap.add_argument("--validated", required=False,
                    help="JSON file mapping task_id -> {owner, conversation_id}")
    ap.add_argument("--drained", action="store_true",
                    help="assert active jobs are drained (no active rows)")
    ap.add_argument("--check", action="store_true", help="only print status")
    args = ap.parse_args()
    if args.check:
        print(json.dumps(read_handoff(args.table), ensure_ascii=False, indent=2))
    else:
        validated = {}
        if args.validated:
            validated = json.loads(Path(args.validated).read_text(encoding="utf-8"))
        table = TaskTable(path=args.table)
        plan = build_handoff_plan(
            [{"task_id": r.task_id} for r in table.active()], validated)
        marker = write_handoff(args.table, plan, drained=args.drained)
        print(json.dumps(marker, ensure_ascii=False, indent=2))
