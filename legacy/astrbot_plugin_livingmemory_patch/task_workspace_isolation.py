# -*- coding: utf-8 -*-
"""Minimal, versioned memory-isolation handshake for sibling plugins.

A managed operations plugin stamps an event extra
(``task_workspace_memory_isolated``). When present, LivingMemory skips recall,
reflection, agent memory tools and passive capture for that request.

Capability advertisement has two layers:

* ``advertise(sp)`` is a retained **no-op compatibility shim**. It previously
  wrote a persisted shared-preferences flag, but the persisted flag was never
  authoritative for authorization and ``SharedPreferences.global_get`` /
  ``global_put`` are async-only, so calling them synchronously emitted
  unawaited-coroutine warnings. The shim is kept so existing call sites do not
  need another ``main.py`` diff.
* ``publish_live(context, provider)`` registers the *loaded LivingMemory
  instance* in the shared in-process capability registry attached to the
  AstrBot ``Context``. This is the authoritative signal: it is validated live
  and revoked on terminate/unload, so a downgraded or unloaded instance can
  never leave a stale positive capability behind.

Boot order is order-independent: if this module loads *before* the workspace
plugin has created its registry, it creates its own compatible (duck-typed)
registry on the shared context. If it cannot, it queues a weakref that the
workspace consumes when it creates its registry. Both registries share the same
``publish``/``revoke``/``available`` contract, so a hot-reloaded workspace class
reuses the existing one instead of dropping live providers.

Capability version: 1
"""
from __future__ import annotations

import weakref

try:  # pragma: no cover - astrbot is always importable in production/tests
    from astrbot.api import logger as _logger
except Exception:  # pragma: no cover - defensive fallback keeps the helper usable
    import logging

    _logger = logging.getLogger("astrbot")

MEMORY_ISOLATION_EXTRA_KEY = "task_workspace_memory_isolated"
# Retained for backward compatibility with importers of the legacy key name.
# Nothing authoritative reads or writes this preference any more.
CAPABILITY_KEY = "livingmemory.capabilities"
CAPABILITY_FEATURE = "memory_isolation_extra"
CAPABILITY_VERSION = 1

# Attribute the caller (task-workspace plugin) attaches to the shared Context.
# It exposes ``publish(provider, version)`` / ``revoke(provider)`` /
# ``available()``. We only duck-type it, so this module has no import coupling.
REGISTRY_ATTR = "_task_workspace_memory_capability"
PENDING_ATTR = "_task_workspace_memory_pending"

# Readiness marker method the LivingMemory plugin must expose (see README).
READINESS_ATTR = "memory_isolation_ready"

# Frozen event binding stamped by the workspace plugin's early filter.
BINDING_EXTRA_KEY = "_task_workspace_binding"


def is_isolated(event) -> bool:
    """Return True when the event is marked for memory isolation."""
    try:
        return bool(event.get_extra(MEMORY_ISOLATION_EXTRA_KEY, False))
    except Exception:
        return False


def _event_binding(event):
    if event is None:
        return None
    try:
        data = event.get_extra(BINDING_EXTRA_KEY)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def advertise(sp) -> None:
    """Compatibility no-op; no persisted flag is written.

    Legacy boot code still calls ``advertise(sp)``. The persisted
    ``livingmemory.capabilities`` flag was never used for authorization (the
    workspace plugin trusts only the live in-process registry published by
    :func:`publish_live`), and ``SharedPreferences.global_get`` /
    ``global_put`` are async-only, so the old synchronous calls emitted
    unawaited-coroutine warnings. Keeping this a no-op avoids a second
    ``main.py`` diff while removing the warning and any stale-flag confusion.

    ``sp`` is accepted and ignored for call-site compatibility.
    """
    return None


def ready(provider) -> bool:
    """Live readiness check used by the registry."""
    if provider is None or getattr(provider, "_terminating", False):
        return False
    checker = getattr(provider, READINESS_ATTR, None)
    if checker is None:
        return False
    try:
        return bool(checker() if callable(checker) else checker)
    except Exception:
        return False


class SharedCapabilityRegistry:
    """Weak, duck-typed registry compatible with the workspace plugin's one."""

    def __init__(self) -> None:
        self._providers: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
        self._tokens: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()

    def publish(self, provider, version: int = CAPABILITY_VERSION, token=None) -> bool:
        if provider is None:
            return False
        try:
            version = int(version)
        except (TypeError, ValueError):
            return False
        if version < CAPABILITY_VERSION:
            return False
        try:
            self._providers[provider] = version
            if token is not None:
                self._tokens[provider] = token
        except TypeError:
            return False
        return ready(provider)

    def revoke(self, provider, token=None) -> None:
        try:
            if token is not None:
                stored = self._tokens.get(provider)
                if stored is not None and stored != token:
                    return
            self._providers.pop(provider, None)
            self._tokens.pop(provider, None)
        except Exception:
            pass

    def available(self) -> bool:
        for provider in list(self._providers.keys()):
            if ready(provider):
                return True
        return False

    def live_providers(self):
        return [p for p in list(self._providers.keys()) if ready(p)]


def _is_registry_like(obj) -> bool:
    return (
        obj is not None
        and callable(getattr(obj, "publish", None))
        and callable(getattr(obj, "revoke", None))
        and callable(getattr(obj, "available", None))
    )


def _consume_pending(context, registry) -> None:
    pending = getattr(context, PENDING_ATTR, None)
    if not isinstance(pending, list) or not pending:
        return
    for ref in list(pending):
        try:
            provider = ref() if callable(ref) else ref
        except Exception:
            provider = None
        if provider is not None:
            try:
                registry.publish(provider)
            except Exception:
                pass
    try:
        setattr(context, PENDING_ATTR, [])
    except Exception:
        pass


def _registry(context):
    """Return a compatible registry, creating/attaching one if needed."""
    if context is None:
        return None
    reg = getattr(context, REGISTRY_ATTR, None)
    if _is_registry_like(reg):
        return reg
    reg = SharedCapabilityRegistry()
    try:
        setattr(context, REGISTRY_ATTR, reg)
    except Exception:
        return None
    existing = getattr(context, REGISTRY_ATTR, reg)
    if not _is_registry_like(existing):
        return None
    _consume_pending(context, existing)
    return existing


def _log_readiness(ok: bool, version) -> None:
    """Log live-registry readiness without leaking provider/token details."""
    try:
        ver = int(version)
    except (TypeError, ValueError):
        ver = CAPABILITY_VERSION
    try:
        if ok:
            _logger.info(
                "memory-isolation capability published live "
                "(capability_version=%s)", ver,
            )
        else:
            _logger.warning(
                "memory-isolation capability not live yet "
                "(capability_version=%s)", ver,
            )
    except Exception:
        pass


def publish_live(context, provider, version: int = CAPABILITY_VERSION,
                 token=None) -> bool:
    """Register the loaded provider in the live in-process registry.

    Only call this *after* the plugin's components and hook guards are fully
    loaded and initialization succeeded, so readiness is truthful. Returns False
    if there is no attachable registry *and* the weakref could not be queued.
    """
    reg = _registry(context)
    if reg is not None:
        try:
            published = bool(reg.publish(provider, version, token))
        except TypeError:
            try:
                published = bool(reg.publish(provider, version))
            except Exception:
                published = False
        except Exception:
            published = False
        _log_readiness(published, version)
        return published
    # No attachable registry (unusual context): queue a weakref fallback.
    if context is None or provider is None:
        _log_readiness(False, version)
        return False
    pending = getattr(context, PENDING_ATTR, None)
    if not isinstance(pending, list):
        pending = []
        try:
            setattr(context, PENDING_ATTR, pending)
        except Exception:
            _log_readiness(False, version)
            return False
    try:
        pending.append(weakref.ref(provider))
    except TypeError:
        _log_readiness(False, version)
        return False
    # Queued for the registry the workspace creates later: not available yet.
    _log_readiness(False, version)
    return False


def revoke_live(context, provider, token=None) -> None:
    reg = getattr(context, REGISTRY_ATTR, None)
    if not _is_registry_like(reg):
        return
    try:
        reg.revoke(provider, token)
    except TypeError:
        try:
            reg.revoke(provider)
        except Exception:
            pass
    except Exception:
        pass


# Shared managed-workspace policy attribute the task-workspace plugin attaches
# to the Context. We only read its synchronous index.
POLICY_CONTEXT_ATTR = "_task_workspace_binding_policy"


def policy_denies_capture(context, event, *, binding=None, current_cid=None) -> bool:
    """Decide whether LivingMemory must suppress all memory for this event.

    Ordering is deliberate and shared by recall, reflection, passive capture and
    the agent memory tools:

    1. A frozen binding stamped by the workspace early filter is authoritative:
       a managed binding denies regardless of any later selection change; a
       verified ordinary binding allows the ordinary daily path.
    2. With no frozen binding we consult the *same* live policy index the early
       filter used (or an authoritative cached CID passed by the caller). Any
       managed CID denies regardless of owner; a nonempty, validated ordinary
       CID allows; a missing selection for a known-managed UMO denies.
    3. Any exception is treated as "potentially managed" and denies. A failure
       must never open memory to a managed workspace.
    """
    try:
        if binding is None:
            binding = _event_binding(event)
        if binding is not None:
            return bool(binding.get("managed"))

        umo = str(getattr(event, "unified_msg_origin", "") or "")
        cid = current_cid
        if cid is None:
            policy = getattr(context, POLICY_CONTEXT_ATTR, None)
            if policy is None:
                # No managed-workspace subsystem at all: ordinary daily memory.
                return False
            cid = policy.current_cid(umo) if umo else ""
        if cid:
            policy = getattr(context, POLICY_CONTEXT_ATTR, None)
            if policy is not None and policy.is_managed_cid(cid):
                return True
            # A nonempty CID that is not managed is a validated ordinary
            # conversation: isolation does not apply.
            return False
        # No current CID at all.
        policy = getattr(context, POLICY_CONTEXT_ATTR, None)
        if policy is not None and policy.is_managed_umo(umo):
            # Known managed UMO with no determinable selection: fail closed.
            return True
        return False
    except Exception:
        # Exceptions must never allow a potentially managed workspace.
        return True
