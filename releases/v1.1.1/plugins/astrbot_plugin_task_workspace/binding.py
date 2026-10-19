# -*- coding: utf-8 -*-
"""Early, synchronous workspace binding for the AstrBot waking stage.

Native ``try_capture_follow_up`` (installed
``astrbot.core.pipeline.process_stage.follow_up``) selects a follow-up run by
``unified_msg_origin`` + sender id only, *before* any ``on_waiting_llm_request``
hook fires. A plugin hook therefore cannot pre-empt it. This module mirrors the
supported extension pattern used by LivingMemory's passive group filter: a
``CustomFilter`` runs synchronously while AstrBot's ``WakingCheckStage``
evaluates adapter-message handlers, i.e. before ``ProcessStage`` and before
native follow-up capture.

The filter never wakes an ordinary message (it always returns ``False``). It
stamps an immutable binding extra on the event and rejects text that would be
captured by a native runner bound to a *different* conversation.
"""
from __future__ import annotations

import weakref

from astrbot.api.event.filter import CustomFilter

BINDING_EXTRA_KEY = "_task_workspace_binding"
BINDING_GUARD_VERSION = 2
POLICY_CONTEXT_ATTR = "_task_workspace_binding_policy"

# Name of the readiness marker the staged core patch defines *inside the
# imported native follow_up module*. A local constant here is not evidence that
# the running core actually has the synchronous pre-capture guard installed.
NATIVE_GUARD_MARKER = "TASK_WORKSPACE_BINDING_GUARD_VERSION"
SUPPORTED_NATIVE_GUARD_VERSION = 2

# Command tokens that must always be allowed to reach their handlers even while
# a cross-conversation native run is active (bridge/management execution).
KNOWN_COMMAND_TOKENS = frozenset(
    {
        "task",
        "wg", "wc", "wl", "ww", "wx", "wd", "nt",
        "switch", "tasks", "close", "rename",
        "help", "reset", "sid",
    }
)

_ACTIVE_PLUGIN_REF: weakref.ReferenceType | None = None


def set_active_plugin(plugin) -> None:
    global _ACTIVE_PLUGIN_REF
    _ACTIVE_PLUGIN_REF = weakref.ref(plugin) if plugin is not None else None


def get_active_plugin():
    if _ACTIVE_PLUGIN_REF is None:
        return None
    try:
        return _ACTIVE_PLUGIN_REF()
    except Exception:
        return None


def get_binding(event):
    """Read the immutable binding stamped on an event, if any."""
    try:
        data = event.get_extra(BINDING_EXTRA_KEY)
    except Exception:
        return None
    if isinstance(data, dict):
        return data
    return None


def native_guard_status() -> tuple[bool, int]:
    """Read the guard readiness marker from the *imported* native module.

    File existence or the plugin's own ``BINDING_GUARD_VERSION`` constant prove
    nothing: the running core must expose the exact supported marker on
    ``astrbot.core.pipeline.process_stage.follow_up``. Returns
    ``(installed, version)``.
    """
    try:
        from astrbot.core.pipeline.process_stage import follow_up as _fu
    except Exception:
        return False, 0
    raw = getattr(_fu, NATIVE_GUARD_MARKER, None)
    try:
        version = int(raw)
    except (TypeError, ValueError):
        return False, 0
    return version == SUPPORTED_NATIVE_GUARD_VERSION, version


def active_runner_event(umo: str):
    """Return the event of the active native agent runner for ``umo``.

    Reads the private, versioned core registry. Absence of the registry is
    reported as unsupported (``None``) rather than treated as "no runner".
    """
    try:
        from astrbot.core.pipeline.process_stage import follow_up as _fu
    except Exception:
        return None
    registry = getattr(_fu, "_ACTIVE_AGENT_RUNNERS", None)
    if not isinstance(registry, dict):
        return None
    runner = registry.get(umo)
    if runner is None:
        return None
    ctx = getattr(runner, "run_context", None)
    ctx = getattr(ctx, "context", None)
    return getattr(ctx, "event", None)


def active_runner_binding(umo: str):
    ev = active_runner_event(umo)
    if ev is None:
        return None
    return get_binding(ev)


def _looks_like_command(event) -> bool:
    try:
        text = (event.get_message_str() or "").strip()
    except Exception:
        return False
    if not text:
        return False
    token = text.lstrip("/").split(None, 1)[0].lstrip("/").lower()
    return token in KNOWN_COMMAND_TOKENS


looks_like_command = _looks_like_command


class BindingPolicy:
    """Synchronous, hydrated index of managed conversations.

    Hydration is asynchronous (done by the manager); reads happen in the sync
    filter. A missing cache for a known-managed UMO must fail closed, never be
    treated as ordinary chat.
    """

    def __init__(self) -> None:
        self._by_cid: dict[str, dict] = {}
        self._by_umo: dict[str, set] = {}
        self._current: dict[str, str] = {}
        self._cm = None
        self._version = 0

    def bind_conversation_manager(self, cm) -> None:
        """Attach the shared ConversationManager.

        The synchronous filter must consult the *authoritative* live selection
        (``cm.session_conversations``), not a plugin-private copy: native UI,
        ``/new`` and ``/reset`` switch conversations without telling the plugin.
        """
        self._cm = cm

    def load(self, workspaces) -> None:
        by_cid: dict[str, dict] = {}
        by_umo: dict[str, set] = {}

        def field(ws, name, default=""):
            if isinstance(ws, dict):
                return ws.get(name, default)
            return getattr(ws, name, default)

        for ws in workspaces or []:
            rec = {
                "ws_id": field(ws, "id", ""),
                "cid": field(ws, "conversation_id", ""),
                "umo": field(ws, "umo", ""),
                "owner": field(ws, "owner_user_id", ""),
                "archived": int(field(ws, "archived", 0) or 0),
                "environment": field(ws, "environment", "windows_remote"),
                "cwd": field(ws, "cwd", ""),
                "title": field(ws, "title", ""),
            }
            if not rec["cid"]:
                continue
            by_cid[rec["cid"]] = rec
            by_umo.setdefault(rec["umo"], set()).add(rec["cid"])
        self._by_cid = by_cid
        self._by_umo = by_umo
        self._version += 1

    def set_current(self, umo: str, cid: str) -> None:
        if umo and cid:
            self._current[umo] = cid

    def forget_current(self, umo: str) -> None:
        self._current.pop(umo, None)

    def current_cid(self, umo: str) -> str:
        """Return the authoritative current CID for ``umo`` (synchronous).

        ``cm.session_conversations`` is the live selection the native UI,
        ``/new`` and ``/reset`` mutate; when the ConversationManager is bound we
        never fall back to a stale plugin-private copy.
        """
        cm = self._cm
        live = getattr(cm, "session_conversations", None) if cm is not None else None
        if isinstance(live, dict):
            cid = live.get(umo, "")
            if cid:
                self._current[umo] = cid
                return cid
            # Live authority reports no selection: do not leak a stale pointer.
            return ""
        return self._current.get(umo, "")

    def is_managed_umo(self, umo: str) -> bool:
        return bool(self._by_umo.get(umo))

    def is_managed_cid(self, cid: str) -> bool:
        return bool(cid) and cid in self._by_cid

    def workspace_for_cid(self, cid: str):
        return self._by_cid.get(cid)

    @property
    def version(self) -> int:
        return self._version


class TaskWorkspaceBindingFilter(CustomFilter):
    """Synchronous early filter that stamps or rejects workspace bindings."""

    def __init__(self, raise_error: bool = True, plugin=None, **kwargs) -> None:
        if not isinstance(raise_error, bool) and plugin is None:
            plugin = raise_error
            raise_error = True
        super().__init__(raise_error=raise_error, **kwargs)
        self._plugin_ref = weakref.ref(plugin) if plugin is not None else None

    def _get_plugin(self):
        if self._plugin_ref is not None:
            return self._plugin_ref()
        return get_active_plugin()

    def filter(self, event, cfg) -> bool:
        plugin = self._get_plugin()
        if plugin is not None:
            try:
                plugin.on_adapter_message(event)
            except Exception:
                # A guard failure must never crash the pipeline; the managed
                # path is re-checked and fails closed in later hooks.
                pass
        # Never wake an ordinary message: this handler stays inert.
        return False


def dual_runtime_ready() -> bool:
    try:
        from astrbot.core.workspace_runtime import RUNTIME_VERSION
        return RUNTIME_VERSION == 2 and native_guard_status()[0]
    except ImportError:
        return False
