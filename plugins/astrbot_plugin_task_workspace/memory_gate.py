# -*- coding: utf-8 -*-
"""Memory isolation gate for managed operations workspaces.

LivingMemory (v2.6.0-beta.3) exposes no per-event isolation predicate unless the
staged isolation patch is deployed. The task-workspace plugin stamps every
managed request event with ``MEMORY_ISOLATION_EXTRA_KEY`` in the earliest
adapter-message filter, freezing identity before any await that could permit a
conversation switch; the patched LivingMemory honours that extra and skips
recall, reflection, agent memory tools and passive capture for the event.

Whether that contract can be relied upon is decided by the *live in-process*
capability registry (:mod:`memory_capability`), not by a persisted preference
flag. The registry only reports available while a loaded LivingMemory instance
advertises readiness; unloading/downgrading revokes it immediately, so a stale
positive bit can never keep a managed workspace executing.
"""
from __future__ import annotations

import time

from .memory_capability import (
    CAPABILITY_VERSION,
    MemoryCapabilityRegistry,
    get_registry,
)

MEMORY_ISOLATION_EXTRA_KEY = "task_workspace_memory_isolated"

# Kept for backwards compatibility with existing configuration/tests. These are
# no longer treated as authoritative.
CAPABILITY_SCOPE = "global"
CAPABILITY_KEY = "livingmemory.capabilities"
CAPABILITY_FEATURE = "memory_isolation_extra"
MIN_CAPABILITY_VERSION = CAPABILITY_VERSION


class MemoryCapability:
    def __init__(self, available: bool, mechanism: str, version: int = 0):
        self.available = available
        self.mechanism = mechanism
        self.version = version

    def __bool__(self):
        return self.available

    def __repr__(self):
        return (
            f"MemoryCapability(available={self.available}, "
            f"mechanism={self.mechanism!r}, version={self.version})"
        )


class MemoryGate:
    """Decide whether memory isolation is enforceable for managed requests.

    Authority order:

    1. an explicit ``capability_probe`` (tests / custom hosts);
    2. the live in-process registry (loaded LivingMemory instance readiness).

    The persisted shared-preferences flag is intentionally ignored: it survives
    unload/downgrade and would cache a positive capability forever.
    """

    def __init__(self, sp=None, capability_probe=None, registry=None, ttl: float = 0.5):
        self._sp = sp
        self._probe = capability_probe
        self._registry = registry
        self._cached: MemoryCapability | None = None
        self._cached_at = 0.0
        self._generation = 0
        self._cached_generation = -1
        self._ttl = ttl

    def attach_context(self, context) -> None:
        """Bind the gate to the shared AstrBot Context registry."""
        self._registry = get_registry(context)

    @property
    def registry(self) -> MemoryCapabilityRegistry | None:
        return self._registry

    def invalidate(self):
        """Revoke any cached capability (plugin reload/downgrade/unload)."""
        self._cached = None
        self._cached_at = 0.0
        self._generation += 1

    def detect(self, use_cache: bool = True) -> MemoryCapability:
        # The live registry is always consulted fresh: a positive capability
        # must never survive a revoke via a TTL cache.
        if self._registry is not None:
            if self._registry.available():
                return MemoryCapability(
                    True, "livingmemory.live_instance", CAPABILITY_VERSION
                )
            # A negative result may be cached briefly, but invalidate() drops it
            # so an initial absence remains re-checkable.
        now = time.time()
        fresh = (
            self._cached is not None
            and self._cached_generation == self._generation
            and (now - self._cached_at) < self._ttl
        )
        if use_cache and fresh:
            return self._cached
        cap = self._detect()
        self._cached = cap
        self._cached_at = now
        self._cached_generation = self._generation
        return cap

    def _detect(self) -> MemoryCapability:
        if self._probe is not None:
            try:
                result = self._probe()
                if isinstance(result, MemoryCapability):
                    return result
                if isinstance(result, bool):
                    return MemoryCapability(
                        result, "custom_probe", MIN_CAPABILITY_VERSION if result else 0
                    )
            except Exception:
                pass
        if self._registry is not None and self._registry.available():
            return MemoryCapability(
                True, "livingmemory.live_instance", CAPABILITY_VERSION
            )
        return MemoryCapability(False, "unavailable", 0)

    @staticmethod
    def stamp(event) -> bool:
        """Mark the event isolated. Returns False on failure (caller fails closed)."""
        try:
            event.set_extra(MEMORY_ISOLATION_EXTRA_KEY, True)
            return bool(event.get_extra(MEMORY_ISOLATION_EXTRA_KEY, False))
        except Exception:
            return False

    def execution_allowed(self) -> tuple[bool, str]:
        cap = self.detect()
        if cap.available:
            return True, cap.mechanism
        return (
            False,
            "记忆隔离能力不可用（未检测到已加载的 livingmemory 隔离实例）；"
            "按 fail-closed 策略拒绝执行。需部署 staged livingmemory 隔离补丁。",
        )
