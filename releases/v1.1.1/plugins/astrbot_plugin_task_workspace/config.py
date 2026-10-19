# -*- coding: utf-8 -*-
"""Configuration accessor.

The plugin is fail-closed: ``enabled`` defaults to False and
``allowed_sessions`` defaults to an empty list, which authorises nobody. Both
must be configured explicitly before any execution or management command will
work.
"""
from __future__ import annotations

from pathlib import Path


class TaskWorkspaceConfig:
    DEFAULTS = {
        "enabled": False,
        "allowed_sessions": [],
        "require_admin_private": True,
        "max_global_runs": 2,
        "default_permission": "workspace-write",
        "default_timeout_s": 600,
        "default_executor": "dsh",
        "workspace_persona": "运维",
        "context_max_chars": 6000,
        "result_max_chars": 4000,
        "notify_interval": 10,
        "ssh": {},
        "workspace_root": "",
        "poll_interval": 15,
        "announce_interval": 30,
        "max_text_len": 1500,
        "verbose": False,
        "require_native_guard": False,
        "require_legacy_handoff": False,
        "managed_chat_allowed_tools": [],
    }

    def __init__(self, config=None):
        self._cfg = config if isinstance(config, dict) else {}

    def _raw(self, key, default=None):
        try:
            val = self._cfg.get(key)
            return default if val in (None, "") else val
        except Exception:
            return default

    @staticmethod
    def _from(container, key):
        try:
            val = container.get(key)
            return val if val not in (None, "") else None
        except Exception:
            return None

    def _ssh(self, key):
        nested = self._raw("ssh", None)
        if isinstance(nested, dict):
            return self._from(nested, key)
        return self._raw(key, None)

    @property
    def enabled(self) -> bool:
        try:
            return bool(self._raw("enabled", self.DEFAULTS["enabled"]))
        except Exception:
            return False

    @property
    def require_admin_private(self) -> bool:
        try:
            return bool(self._raw("require_admin_private", True))
        except Exception:
            return True

    @property
    def allowed_sessions(self) -> list[str]:
        val = self._raw("allowed_sessions", self.DEFAULTS["allowed_sessions"])
        if isinstance(val, str):
            return [s.strip() for s in val.splitlines() if s.strip()]
        if isinstance(val, (list, tuple)):
            return [str(s).strip() for s in val if str(s).strip()]
        return []

    @property
    def max_global_runs(self) -> int:
        try:
            return max(1, int(self._raw("max_global_runs", 2)))
        except Exception:
            return 2

    @property
    def default_permission(self) -> str:
        return str(self._raw("default_permission", "workspace-write"))

    @property
    def default_timeout_s(self) -> int:
        try:
            return max(1, int(self._raw("default_timeout_s", 600)))
        except Exception:
            return 600

    @property
    def default_executor(self) -> str:
        val = str(self._raw("default_executor", "dsh"))
        return val if val in ("dsh", "codex") else "dsh"

    @property
    def workspace_persona(self) -> str:
        return str(self._raw("workspace_persona", "运维"))

    @property
    def context_max_chars(self) -> int:
        try:
            return max(500, int(self._raw("context_max_chars", 6000)))
        except Exception:
            return 6000

    @property
    def result_max_chars(self) -> int:
        try:
            return max(200, int(self._raw("result_max_chars", 4000)))
        except Exception:
            return 4000

    @property
    def notify_interval(self) -> int:
        try:
            return max(2, int(self._raw("notify_interval", 10)))
        except Exception:
            return 10

    @property
    def poll_interval(self) -> int:
        try:
            return max(3, int(self._raw("poll_interval", 15)))
        except Exception:
            return 15

    @property
    def announce_interval(self) -> int:
        try:
            return max(5, int(self._raw("announce_interval", 30)))
        except Exception:
            return 30

    @property
    def max_text_len(self) -> int:
        try:
            return max(50, int(self._raw("max_text_len", 1500)))
        except Exception:
            return 1500

    @property
    def verbose(self) -> bool:
        try:
            return bool(self._raw("verbose", False))
        except Exception:
            return False

    @property
    def require_native_guard(self) -> bool:
        """Refuse execution unless the staged native follow-up guard is loaded."""
        try:
            return bool(self._raw("require_native_guard", False))
        except Exception:
            return False

    @property
    def require_legacy_handoff(self) -> bool:
        """Demand a verified legacy-bridge handoff before new execution.

        Default ``False`` for standalone installs. The parent sets it ``True``
        when migrating an existing legacy bridge, so a workspace-first boot
        denies execution until the bridge publishes readiness (a missing
        HandoffState must not be treated as "no legacy source").
        """
        try:
            return bool(self._raw("require_legacy_handoff", False))
        except Exception:
            return False

    @property
    def managed_chat_allowed_tools(self) -> list[str]:
        """Default-deny tool allowlist for managed workspace *native* chat.

        Empty (the default) exposes **no** native tools to a managed workspace
        conversation: the model may only discuss and consume results, and real
        execution must go through an explicit ``/task run`` (remote executor,
        run ledger, per-run permission). Names listed here survive the strip;
        ordinary daily sessions are never touched.
        """
        val = self._raw("managed_chat_allowed_tools",
                        self.DEFAULTS["managed_chat_allowed_tools"])
        if isinstance(val, str):
            return [s.strip() for s in val.splitlines() if s.strip()]
        if isinstance(val, (list, tuple)):
            return [str(s).strip() for s in val if str(s).strip()]
        return []

    @property
    def workspace_root(self) -> str:
        return str(self._raw("workspace_root", "") or "")

    @property
    def ssh_host(self) -> str:
        return str(self._ssh("host") or "")

    @property
    def ssh_port(self) -> int:
        try:
            return int(self._ssh("port") or 22)
        except Exception:
            return 22

    @property
    def ssh_user(self) -> str:
        return str(self._ssh("user") or "")

    @property
    def ssh_work_dir(self) -> str:
        return str(self._ssh("work_dir") or "")

    @property
    def ssh_python(self) -> str:
        return str(self._ssh("python") or "python")

    @property
    def ssh_timeout_s(self) -> int:
        try:
            return max(5, int(self._ssh("timeout_s") or 30))
        except Exception:
            return 30

    @property
    def known_hosts(self) -> str:
        return str(self._ssh("known_hosts") or "")

    @property
    def host_key_policy(self) -> str:
        """``reject`` (default) or explicit ``auto`` opt-in."""
        val = str(self._ssh("host_key_policy") or "reject").lower()
        return val if val in ("reject", "auto") else "reject"

    def session_allowed(self, session: str) -> bool:
        """Empty list means nobody (fail closed). Exact full-UMO membership only."""
        allowed = self.allowed_sessions
        if not allowed:
            return False
        session = str(session or "")
        if not session:
            return False
        return session in allowed

    @property
    def linux_cwd(self) -> str:
        return str(self._raw("linux_cwd", str(Path.cwd())))

    @property
    def linux_max_runs(self) -> int:
        return max(1, int(self._raw("linux_max_runs", 2)))
