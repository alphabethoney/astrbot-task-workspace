# -*- coding: utf-8 -*-
"""Live, in-process memory-isolation capability registry.

The previous implementation trusted a persisted shared-preferences flag with a
short TTL. That flag stays stale forever after the livingmemory plugin is
downgraded or unloaded, so a managed workspace could keep executing against a
livingmemory instance that no longer honours the isolation extra.

This module replaces the persisted flag with an in-process registry that is
attached to the shared AstrBot ``Context`` (all Star plugins receive the same
``context`` instance). LivingMemory publishes itself only *after* its
components and hook guards finished loading; it revokes on terminate/unload.
Authorization is validated live on every probe:

* the provider must still be weakly reachable (unload/GC auto-revokes);
* the provider must not be terminating;
* the provider must expose a readiness marker that returns ``True`` *now*.

A positive result is never cached across a revoke: the live probe is consulted
on every ``is_available`` call, so a downgraded/unloaded instance cannot leave a
stale positive capability behind.
"""
from __future__ import annotations

import weakref

# Bump when the isolation contract changes in a breaking way. Callers require
# ``>= MIN_CAPABILITY_VERSION``.
CAPABILITY_VERSION = 1

# Attribute name on the shared AstrBot Context; both plugins agree on it.
CONTEXT_ATTR = "_task_workspace_memory_capability"

# Attribute holding weakrefs to providers published while no registry existed
# yet. A plugin that loads *first* may attach its registry later; the pending
# list lets the workspace registry discover the already-ready provider instead
# of silently dropping it.
PENDING_ATTR = "_task_workspace_memory_pending"

# Attribute LivingMemory sets on its own instance after successful
# initialization (once components/hook guards are installed) and clears on
# terminate/unload. It must be a callable returning bool.
READINESS_ATTR = "memory_isolation_ready"


def is_registry_like(obj) -> bool:
    """Duck-typed compatibility check for a capability registry.

    We intentionally never use concrete class identity: LivingMemory publishes
    its own registry implementation (a copy of this contract) and a workspace
    hot reload re-imports a *new* class object. A class-identity check would
    replace the compatible registry and lose every already-published provider.
    """
    return (
        obj is not None
        and callable(getattr(obj, "publish", None))
        and callable(getattr(obj, "revoke", None))
        and callable(getattr(obj, "available", None))
    )


class MemoryCapabilityRegistry:
    """Weak registry of loaded providers that can honour memory isolation."""

    def __init__(self) -> None:
        # provider instance -> advertised version
        self._providers: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
        # provider instance -> optional publish token (identity-scoped revoke)
        self._tokens: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()

    def publish(self, provider, version: int = CAPABILITY_VERSION,
                token=None) -> bool:
        """Register a loaded provider; returns True when it is ready now."""
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
            # Not weak-referenceable.
            return False
        return self.is_ready(provider)

    def revoke(self, provider, token=None) -> None:
        """Revoke exactly ``provider``; a stale token cannot revoke a successor."""
        try:
            if token is not None:
                stored = self._tokens.get(provider)
                if stored is not None and stored != token:
                    return
            self._providers.pop(provider, None)
            self._tokens.pop(provider, None)
        except Exception:
            pass

    @staticmethod
    def _ready(provider) -> bool:
        if provider is None:
            return False
        if getattr(provider, "_terminating", False):
            return False
        checker = getattr(provider, READINESS_ATTR, None)
        if checker is None:
            # No readiness marker: cannot trust the instance.
            return False
        try:
            return bool(checker() if callable(checker) else checker)
        except Exception:
            # A marker that raises (e.g. components not loaded) is not ready.
            return False

    def is_ready(self, provider) -> bool:
        return self._ready(provider)

    def available(self) -> bool:
        # Snapshot keys; WeakKeyDictionary may mutate during iteration.
        for provider in list(self._providers.keys()):
            if self._ready(provider):
                return True
        return False

    def live_providers(self):
        return [p for p in list(self._providers.keys()) if self._ready(p)]


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


def get_registry(context) -> MemoryCapabilityRegistry | None:
    """Return (creating if needed) the registry attached to a shared context.

    Any *compatible* registry is reused (duck-typed), including one created by
    the LivingMemory isolation module before this plugin loaded. A workspace hot
    reload therefore keeps the registry and its live providers.
    """
    if context is None:
        return None
    reg = getattr(context, CONTEXT_ATTR, None)
    if is_registry_like(reg):
        return reg
    reg = MemoryCapabilityRegistry()
    try:
        setattr(context, CONTEXT_ATTR, reg)
    except Exception:
        return None
    # Re-read to avoid a race creating two registries.
    existing = getattr(context, CONTEXT_ATTR, reg)
    if not is_registry_like(existing):
        return None
    _consume_pending(context, existing)
    return existing


def register_pending(context, provider, version: int = CAPABILITY_VERSION,
                     token=None) -> bool:
    """Publish a provider even when no registry exists yet.

    If a compatible registry is already present the provider is published into
    it directly; otherwise a weakref is queued and consumed when the workspace
    creates its registry later. This closes the boot-order race where
    LivingMemory loads before the workspace plugin.
    """
    if context is None or provider is None:
        return False
    reg = getattr(context, CONTEXT_ATTR, None)
    if is_registry_like(reg):
        try:
            return bool(reg.publish(provider, version, token))
        except Exception:
            return False
    pending = getattr(context, PENDING_ATTR, None)
    if not isinstance(pending, list):
        pending = []
        try:
            setattr(context, PENDING_ATTR, pending)
        except Exception:
            return False
    try:
        pending.append(weakref.ref(provider))
    except TypeError:
        return False
    return False


def publish(context, provider, version: int = CAPABILITY_VERSION,
            token=None) -> bool:
    reg = get_registry(context)
    if reg is None:
        return register_pending(context, provider, version, token)
    return reg.publish(provider, version, token)


def revoke(context, provider, token=None) -> None:
    reg = getattr(context, CONTEXT_ATTR, None)
    if is_registry_like(reg):
        try:
            reg.revoke(provider, token)
        except TypeError:
            reg.revoke(provider)
        except Exception:
            pass
