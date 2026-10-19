# -*- coding: utf-8 -*-
"""Staged, versioned binding guard for AstrBot's native follow-up capture.

DO NOT apply to a live core tree directly. This module is a standalone,
side-effect-free helper; the accompanying ``follow_up.py.patch`` wires it into
``astrbot.core.pipeline.process_stage.follow_up.try_capture_follow_up``
immediately before ``runner.follow_up(...)`` (no ``await`` in between).

Why a core patch is required
-----------------------------
``try_capture_follow_up`` runs at the start of the internal agent sub-stage,
*before* any plugin ``on_waiting_llm_request`` hook, and selects the active run
by ``umo`` + sender id only. A plugin-only early filter (see
``astrbot_plugin_task_workspace.binding``) closes the common case, but a run can
register during asynchronous preprocessing *after* the filter ran (TOCTOU). This
synchronous comparator, evaluated right before capture with no intervening
await, closes that race deterministically.

Semantics
---------
* Both events ordinary (no managed binding): unchanged native behaviour.
* Either side managed: both must carry a valid binding and agree on
  umo/owner/cid/workspace_id; otherwise capture is denied (returns ``False``)
  and the normal pipeline / native UMO lock / request-CID guard takes over.
* A missing binding when the other side is managed denies capture.
"""

# Bump on breaking contract changes; the plugin may check for this symbol.
TASK_WORKSPACE_BINDING_GUARD_VERSION = 1

BINDING_EXTRA_KEY = "_task_workspace_binding"


def get_event_binding(event):
    if event is None:
        return None
    try:
        data = event.get_extra(BINDING_EXTRA_KEY)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _managed(binding) -> bool:
    return bool(binding and binding.get("managed"))


def binding_guard_allows(incoming_event, runner_event) -> bool:
    """Return True when native follow-up capture is safe for the two events."""
    incoming = get_event_binding(incoming_event)
    runner = get_event_binding(runner_event)

    incoming_managed = _managed(incoming)
    runner_managed = _managed(runner)

    # Both ordinary: leave native behaviour untouched.
    if not incoming_managed and not runner_managed:
        return True

    # One side managed, the other missing/unknown: never capture cross-workspace.
    if incoming is None or runner is None:
        return False

    if incoming.get("umo") != runner.get("umo"):
        return False
    if incoming.get("cid") != runner.get("cid"):
        return False
    if incoming.get("owner") != runner.get("owner"):
        return False
    if incoming.get("ws_id") != runner.get("ws_id"):
        return False

    # Both managed and identical binding: allow the native follow-up.
    return incoming_managed and runner_managed
