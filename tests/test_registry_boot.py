# -*- coding: utf-8 -*-
"""Boot-order and hot-reload tests for the shared memory-capability registry.

The LivingMemory isolation module is a *separate* deployment tree; it publishes
into a duck-typed registry attached to the shared AstrBot Context. The workspace
plugin must reuse that registry (never replace it by concrete class identity) and
must discover providers published before it loaded.
"""
import gc
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[1] / "legacy" / "astrbot_plugin_livingmemory_patch"))

from helpers import FakeProvider  # noqa: E402

import task_workspace_isolation as iso  # noqa: E402
from astrbot_plugin_task_workspace.memory_capability import (  # noqa: E402
    CAPABILITY_VERSION,
    CONTEXT_ATTR,
    MemoryCapabilityRegistry,
    get_registry,
    register_pending,
)
from astrbot_plugin_task_workspace.memory_gate import MemoryGate  # noqa: E402


class Ctx:
    pass


class BootOrderTests(unittest.TestCase):
    def test_lm_first_then_workspace_discovers_ready_provider(self):
        ctx = Ctx()
        provider = FakeProvider(True)
        # LivingMemory loads first, before the workspace created any registry.
        self.assertTrue(iso.publish_live(ctx, provider, CAPABILITY_VERSION))
        self.assertTrue(iso._registry(ctx).available())
        # Workspace loads later: get_registry must reuse the compatible registry.
        reg = get_registry(ctx)
        self.assertTrue(reg.available())
        self.assertIs(reg, getattr(ctx, CONTEXT_ATTR))
        gate = MemoryGate(registry=reg)
        self.assertTrue(gate.detect().available)

    def test_workspace_first_then_lm_publishes_into_it(self):
        ctx = Ctx()
        reg = get_registry(ctx)
        self.assertIsInstance(reg, MemoryCapabilityRegistry)
        provider = FakeProvider(True)
        self.assertTrue(iso.publish_live(ctx, provider, CAPABILITY_VERSION))
        self.assertTrue(reg.available())
        self.assertTrue(MemoryGate(registry=reg).detect().available)

    def test_pending_weakref_consumed_on_registry_creation(self):
        ctx = Ctx()
        provider = FakeProvider(True)
        # No registry yet: provider is queued, not dropped.
        register_pending(ctx, provider)
        reg = get_registry(ctx)
        self.assertTrue(reg.available())

    def test_hot_reload_reuses_compatible_registry(self):
        ctx = Ctx()
        provider = FakeProvider(True)
        iso.publish_live(ctx, provider, CAPABILITY_VERSION)
        first = get_registry(ctx)
        # A hot reload re-imports a *new* class object; duck typing must keep the
        # same registry instance and its live provider.
        second = get_registry(ctx)
        self.assertIs(first, second)
        self.assertTrue(second.available())

    def test_lm_unload_revokes_immediately(self):
        ctx = Ctx()
        provider = FakeProvider(True)
        iso.publish_live(ctx, provider, CAPABILITY_VERSION)
        gate = MemoryGate(registry=get_registry(ctx))
        self.assertTrue(gate.detect().available)
        provider._ready = False
        self.assertFalse(gate.detect().available)
        iso.revoke_live(ctx, provider)
        self.assertFalse(get_registry(ctx).available())

    def test_replacement_survives_old_instance_terminate(self):
        ctx = Ctx()
        old = FakeProvider(True)
        new = FakeProvider(True)
        iso.publish_live(ctx, old, CAPABILITY_VERSION)
        iso.publish_live(ctx, new, CAPABILITY_VERSION)
        # Old instance terminates: revoke must remove only the old provider.
        old._terminating = True
        iso.revoke_live(ctx, old)
        reg = get_registry(ctx)
        self.assertTrue(reg.available())
        self.assertEqual([p for p in reg.live_providers()], [new])

    def test_weakref_gc_revokes(self):
        ctx = Ctx()
        provider = FakeProvider(True)
        iso.publish_live(ctx, provider, CAPABILITY_VERSION)
        self.assertTrue(get_registry(ctx).available())
        del provider
        gc.collect()
        self.assertFalse(get_registry(ctx).available())

    def test_stale_token_does_not_revoke_newer_publish(self):
        reg = MemoryCapabilityRegistry()
        provider = FakeProvider(True)
        reg.publish(provider, CAPABILITY_VERSION, token="old")
        reg.publish(provider, CAPABILITY_VERSION, token="new")
        reg.revoke(provider, token="old")  # stale token: no-op
        self.assertTrue(reg.available())
        reg.revoke(provider, token="new")
        self.assertFalse(reg.available())


if __name__ == "__main__":
    unittest.main()
