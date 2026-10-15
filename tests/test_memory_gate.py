# -*- coding: utf-8 -*-
"""Tests for the live, revocable memory-isolation capability."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import FakeEvent, FakeProvider, FakeSp  # noqa: E402

from astrbot_plugin_task_workspace.memory_capability import (  # noqa: E402
    CAPABILITY_VERSION,
    CONTEXT_ATTR,
    READINESS_ATTR,
    MemoryCapabilityRegistry,
    get_registry,
)
from astrbot_plugin_task_workspace.memory_gate import (  # noqa: E402
    MEMORY_ISOLATION_EXTRA_KEY,
    MemoryGate,
)


class MemoryGateTests(unittest.TestCase):
    def test_absent_capability_blocks_execution(self):
        gate = MemoryGate(registry=MemoryCapabilityRegistry())
        self.assertFalse(gate.detect().available)
        ok, reason = gate.execution_allowed()
        self.assertFalse(ok)
        self.assertIn("隔离能力", reason)

    def test_present_capability_allows(self):
        reg = MemoryCapabilityRegistry()
        provider = FakeProvider(True)
        reg.publish(provider, CAPABILITY_VERSION)
        gate = MemoryGate(registry=reg)
        ok, _ = gate.execution_allowed()
        self.assertTrue(ok)

    def test_probe_override(self):
        gate = MemoryGate(capability_probe=lambda: True)
        self.assertTrue(gate.detect().available)
        gate2 = MemoryGate(capability_probe=lambda: False)
        self.assertFalse(gate2.detect().available)

    def test_stamp_sets_extra(self):
        ev = FakeEvent()
        MemoryGate.stamp(ev)
        self.assertTrue(ev.get_extra(MEMORY_ISOLATION_EXTRA_KEY))

    def test_persisted_flag_is_not_authoritative(self):
        sp = FakeSp()
        sp.global_put("livingmemory.capabilities", {"memory_isolation_extra": 1})
        gate = MemoryGate(sp=sp, registry=MemoryCapabilityRegistry())
        self.assertFalse(gate.detect().available)


class LiveCapabilityTests(unittest.TestCase):
    def test_downgrade_unload_reload_new_instance(self):
        reg = MemoryCapabilityRegistry()
        old = FakeProvider(True)
        self.assertTrue(reg.publish(old, CAPABILITY_VERSION))
        self.assertTrue(reg.available())

        # Unload/downgrade: readiness revoked -> unavailable immediately, with
        # no TTL window during which a stale positive survives.
        old._ready = False
        self.assertFalse(reg.available())
        reg.revoke(old)
        self.assertFalse(reg.available())

        # New compatible instance becomes ready later.
        new = FakeProvider(True)
        gate = MemoryGate(registry=reg)
        self.assertFalse(gate.detect().available)  # negative may be cached
        reg.publish(new, CAPABILITY_VERSION)
        gate.invalidate()
        self.assertTrue(gate.detect().available)

    def test_initial_unavailable_later_ready(self):
        reg = MemoryCapabilityRegistry()
        gate = MemoryGate(registry=reg)
        self.assertFalse(gate.detect().available)
        provider = FakeProvider(True)
        reg.publish(provider, CAPABILITY_VERSION)
        gate.invalidate()
        self.assertTrue(gate.detect().available)

    def test_marker_write_failure_fails_closed(self):
        reg = MemoryCapabilityRegistry()
        broken = FakeProvider(True)
        setattr(broken, READINESS_ATTR, lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        reg.publish(broken, CAPABILITY_VERSION)
        self.assertFalse(reg.available())

        # Removing the marker entirely is also untrusted.
        class NoMarker:
            _terminating = False
        reg2 = MemoryCapabilityRegistry()
        reg2.publish(NoMarker(), CAPABILITY_VERSION)
        self.assertFalse(reg2.available())

    def test_weakref_auto_revokes_on_gc(self):
        import gc

        reg = MemoryCapabilityRegistry()
        provider = FakeProvider(True)
        reg.publish(provider, CAPABILITY_VERSION)
        self.assertTrue(reg.available())
        del provider
        gc.collect()
        self.assertFalse(reg.available())

    def test_registry_shared_via_context(self):
        class Ctx:
            pass

        ctx = Ctx()
        r1 = get_registry(ctx)
        r2 = get_registry(ctx)
        self.assertIs(r1, r2)
        self.assertTrue(hasattr(ctx, CONTEXT_ATTR))


if __name__ == "__main__":
    unittest.main()
