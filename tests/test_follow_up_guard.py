# -*- coding: utf-8 -*-
"""Tests for the staged native follow-up binding guard.

These run against the real installed AstrBot module and the plugin's real early
filter; the guard itself lives in ``legacy/astrbot_core_patch`` because it must
be staged into core by the parent, never applied live here.
"""
import asyncio
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(ROOT / "legacy" / "astrbot_core_patch"))

from helpers import FakeEvent, make_config, native_guard  # noqa: E402
from test_manager import build_manager  # noqa: E402

from follow_up_guard import (  # noqa: E402
    BINDING_EXTRA_KEY,
    TASK_WORKSPACE_BINDING_GUARD_VERSION,
    binding_guard_allows,
)

import astrbot.core.pipeline.process_stage.follow_up as fu  # noqa: E402
from astrbot_plugin_task_workspace.binding import (  # noqa: E402
    NATIVE_GUARD_MARKER,
    SUPPORTED_NATIVE_GUARD_VERSION,
    native_guard_status,
)
from astrbot_plugin_task_workspace.main import TaskWorkspace  # noqa: E402


def _binding(umo, cid, ws_id, owner, managed):
    return {"umo": umo, "cid": cid, "ws_id": ws_id, "owner": owner, "managed": managed}


class FakeRunner:
    def __init__(self, event):
        self.run_context = types.SimpleNamespace(
            context=types.SimpleNamespace(event=event))

    def follow_up(self, message_text=""):
        return object()

    def request_stop(self):
        pass


class GuardUnitTests(unittest.TestCase):
    def test_version_marker(self):
        self.assertEqual(TASK_WORKSPACE_BINDING_GUARD_VERSION, 1)

    def _ev(self, b=None):
        ev = FakeEvent()
        if b is not None:
            ev.set_extra(BINDING_EXTRA_KEY, b)
        return ev

    def test_both_ordinary_allows(self):
        a = self._ev(_binding("u", "cA", "", "", False))
        b = self._ev(_binding("u", "cB", "", "", False))
        self.assertTrue(binding_guard_allows(a, b))

    def test_managed_mismatch_denies(self):
        a = self._ev(_binding("u", "cA", "wsA", "1", True))
        b = self._ev(_binding("u", "cB", "wsB", "1", True))
        self.assertFalse(binding_guard_allows(a, b))

    def test_missing_binding_when_managed_denies(self):
        a = self._ev(_binding("u", "cA", "wsA", "1", True))
        b = self._ev(None)
        self.assertFalse(binding_guard_allows(a, b))
        self.assertFalse(binding_guard_allows(b, a))

    def test_matching_managed_allows(self):
        a = self._ev(_binding("u", "cA", "wsA", "1", True))
        b = self._ev(_binding("u", "cA", "wsA", "1", True))
        self.assertTrue(binding_guard_allows(a, b))

    def test_actual_registry_integration(self):
        umo = "aiocqhttp:private:10001"
        event_a = self._ev(_binding(umo, "cA", "wsA", "10001", True))
        event_b = self._ev(_binding(umo, "cB", "wsB", "10001", True))
        fu._ACTIVE_AGENT_RUNNERS[umo] = FakeRunner(event_a)
        try:
            stored = fu._ACTIVE_AGENT_RUNNERS.get(umo)
            runner_event = stored.run_context.context.event
            # B must not be captured by A's active native runner.
            self.assertFalse(binding_guard_allows(event_b, runner_event))
            self.assertTrue(binding_guard_allows(event_a, runner_event))
        finally:
            fu._ACTIVE_AGENT_RUNNERS.pop(umo, None)


class FilterIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """Invoke the real plugin filter, then the guard against a registered runner."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = make_config()
        self.mgr, self.store, self.ctx, self.sp = build_manager(
            self.tmp.name, config=self.cfg)
        await self.store.migrate()
        self.plugin = TaskWorkspace.__new__(TaskWorkspace)
        self.plugin._manager = self.mgr
        self.plugin._cfg = self.cfg

    async def asyncTearDown(self):
        await self.mgr.shutdown()
        self.store.close()
        self.tmp.cleanup()

    async def test_filter_stamps_then_guard_denies_cross_cid(self):
        umo = "aiocqhttp:private:10001"
        ev = FakeEvent()
        with native_guard(SUPPORTED_NATIVE_GUARD_VERSION):
            # Create workspace A (becomes current) and stamp its binding.
            ws_a = await self.mgr.create_workspace(ev, "A")
            await self.mgr.refresh_policy()
            event_a = FakeEvent()
            self.plugin.on_adapter_message(event_a)
            self.assertTrue(event_a.get_extra(BINDING_EXTRA_KEY)["managed"])
            self.assertEqual(event_a.get_extra(BINDING_EXTRA_KEY)["cid"],
                             ws_a.conversation_id)

            # Switch current to workspace B and run the real filter for a new
            # message.
            ws_b = await self.mgr.create_workspace(ev, "B")
            await self.mgr.refresh_policy()
            event_b = FakeEvent()
            self.plugin.on_adapter_message(event_b)
            self.assertEqual(event_b.get_extra(BINDING_EXTRA_KEY)["cid"],
                             ws_b.conversation_id)

            # Register A as the active native runner, then re-run the filter for
            # B: it must stop the event before native capture.
            fu._ACTIVE_AGENT_RUNNERS[umo] = FakeRunner(event_a)
            try:
                event_b2 = FakeEvent()
                self.plugin.on_adapter_message(event_b2)
                self.assertTrue(event_b2.is_stopped())
                self.assertFalse(binding_guard_allows(
                    event_b2, event_a))
            finally:
                fu._ACTIVE_AGENT_RUNNERS.pop(umo, None)

    async def test_declared_ordinary_umo_not_managed(self):
        other = FakeEvent(umo="aiocqhttp:private:20002", sender="20002")
        self.plugin.on_adapter_message(other)
        b = other.get_extra(BINDING_EXTRA_KEY)
        self.assertIsNotNone(b)
        self.assertFalse(b["managed"])


class ManagedNativeChatGateTests(unittest.IsolatedAsyncioTestCase):
    """Item 1: the guard gates EVERY managed native chat, not just active runs.

    A selected managed workspace with an *empty* active-runner registry must
    still require the loaded guard before it may be stamped for the LLM.
    """

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = make_config()
        self.mgr, self.store, self.ctx, self.sp = build_manager(
            self.tmp.name, config=self.cfg)
        await self.store.migrate()
        self.plugin = TaskWorkspace.__new__(TaskWorkspace)
        self.plugin._manager = self.mgr
        self.plugin._cfg = self.cfg

    async def asyncTearDown(self):
        await self.mgr.shutdown()
        self.store.close()
        self.tmp.cleanup()

    async def _managed_umo(self):
        ev = FakeEvent()
        ws = await self.mgr.create_workspace(ev, "ws")
        await self.mgr.refresh_policy()
        self.assertEqual(self.mgr.classify(ev), "managed")
        return ev, ws

    async def test_missing_guard_with_empty_runner_registry_denies(self):
        await self._managed_umo()
        self.assertEqual(fu._ACTIVE_AGENT_RUNNERS, {})
        with native_guard(None):
            ev = FakeEvent()
            self.plugin.on_adapter_message(ev)
        self.assertTrue(ev.is_stopped())
        # No managed binding was stamped, so no native LLM request can proceed.
        self.assertIsNone(ev.get_extra(BINDING_EXTRA_KEY))

    async def test_loaded_guard_with_empty_runner_registry_allows(self):
        await self._managed_umo()
        self.assertEqual(fu._ACTIVE_AGENT_RUNNERS, {})
        with native_guard(SUPPORTED_NATIVE_GUARD_VERSION):
            ev = FakeEvent()
            self.plugin.on_adapter_message(ev)
        self.assertFalse(ev.is_stopped())
        binding = ev.get_extra(BINDING_EXTRA_KEY)
        self.assertIsNotNone(binding)
        self.assertTrue(binding["managed"])

    async def test_management_command_allowed_without_guard(self):
        await self._managed_umo()
        with native_guard(None):
            ev = FakeEvent(message="/task status")
            self.plugin.on_adapter_message(ev)
        self.assertFalse(ev.is_stopped())

    async def test_bind_request_defense_requires_guard(self):
        await self._managed_umo()
        with native_guard(None):
            ev = FakeEvent()
            await self.plugin._bind_request(ev)
        self.assertTrue(ev.is_stopped())

    async def test_surface_result_defense_requires_guard(self):
        _ev, ws = await self._managed_umo()
        with native_guard(None):
            ev = FakeEvent()
            ev.set_extra(BINDING_EXTRA_KEY, _binding(
                ev.unified_msg_origin, ws.conversation_id, ws.id, "10001", True))
            req = type("Req", (), {
                "conversation": type("C", (), {"cid": ws.conversation_id})()})()
            await self.plugin._surface_result(ev, req)
        self.assertTrue(ev.is_stopped())


MARKER_LINE = "TASK_WORKSPACE_BINDING_GUARD_VERSION = 1"


class PatchValidityTests(unittest.TestCase):
    """The core patch must still apply to a pristine baseline module.

    The live core may already be patched. Rather than skipping (which would drop
    core correctness coverage), a reversible baseline is recovered on an isolated
    copy by reverse-applying the patch when the marker is already present, then
    the patch is applied forward and the native marker is asserted.
    """

    def _patch_binary(self):
        exe = shutil.which("patch")
        if not exe:
            self.skipTest("'patch' tool not available")
        return exe

    def test_patch_applies_cleanly_to_reversible_baseline(self):
        patch_bin = self._patch_binary()
        installed = Path(fu.__file__)
        if not installed.exists():
            self.skipTest("installed AstrBot follow_up.py not found")
        patch_file = ROOT / "legacy/astrbot_core_patch/follow_up.py.patch"
        tmp = tempfile.mkdtemp()
        try:
            dst = Path(tmp) / "core/pipeline/process_stage/follow_up.py"
            dst.parent.mkdir(parents=True)
            shutil.copy(installed, dst)
            text = dst.read_text(encoding="utf-8")
            if MARKER_LINE in text:
                # Already-patched live core: reverse to recover pristine baseline.
                reverse = subprocess.run(
                    [patch_bin, "-R", "-p1", "-i", str(patch_file)],
                    cwd=tmp, capture_output=True, text=True)
                self.assertEqual(reverse.returncode, 0,
                                 f"reverse patch failed: {reverse.stdout}{reverse.stderr}")
                self.assertNotIn(MARKER_LINE, dst.read_text(encoding="utf-8"))
            # Baseline established: the forward patch must apply cleanly.
            dry = subprocess.run(
                [patch_bin, "-p1", "--dry-run", "-i", str(patch_file)],
                cwd=tmp, capture_output=True, text=True)
            self.assertEqual(dry.returncode, 0,
                             f"patch failed: {dry.stdout}{dry.stderr}")
            applied = subprocess.run(
                [patch_bin, "-p1", "-i", str(patch_file)],
                cwd=tmp, capture_output=True, text=True)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            patched = dst.read_text(encoding="utf-8")
            self.assertIn(MARKER_LINE, patched)
            self.assertIn("binding_guard_allows", patched)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_live_core_is_baseline_or_supported(self):
        # Explicit already-patched detection: the imported native module must be
        # either unpatched (0) or the exact supported version; never a mismatch.
        installed, version = native_guard_status()
        if installed:
            self.assertEqual(version, SUPPORTED_NATIVE_GUARD_VERSION)
        else:
            self.assertEqual(version, 0)


class NativeGuardReadinessTests(unittest.TestCase):
    """The plugin must read the marker from the *imported* native module."""

    def setUp(self):
        self._had = hasattr(fu, NATIVE_GUARD_MARKER)
        self._old = getattr(fu, NATIVE_GUARD_MARKER, None)

    def tearDown(self):
        if self._had:
            setattr(fu, NATIVE_GUARD_MARKER, self._old)
        elif hasattr(fu, NATIVE_GUARD_MARKER):
            delattr(fu, NATIVE_GUARD_MARKER)

    def test_unpatched_native_module_rejects(self):
        if hasattr(fu, NATIVE_GUARD_MARKER):
            delattr(fu, NATIVE_GUARD_MARKER)
        installed, version = native_guard_status()
        self.assertFalse(installed)
        self.assertEqual(version, 0)

    def test_loaded_guard_marker_allows(self):
        setattr(fu, NATIVE_GUARD_MARKER, SUPPORTED_NATIVE_GUARD_VERSION)
        installed, version = native_guard_status()
        self.assertTrue(installed)
        self.assertEqual(version, SUPPORTED_NATIVE_GUARD_VERSION)

    def test_wrong_version_rejects(self):
        setattr(fu, NATIVE_GUARD_MARKER, SUPPORTED_NATIVE_GUARD_VERSION + 1)
        installed, _ = native_guard_status()
        self.assertFalse(installed)


class InertFilterRegistrationTests(unittest.TestCase):
    def test_binding_filter_registered_and_inert(self):
        from astrbot.core.star.star_handler import star_handlers_registry
        from astrbot_plugin_task_workspace.binding import TaskWorkspaceBindingFilter

        handler = None
        for h in star_handlers_registry._handlers:
            if h.handler_name == "_workspace_binding_filter":
                handler = h
                break
        self.assertIsNotNone(handler, "binding filter not registered")
        filters = [f for f in handler.event_filters
                   if isinstance(f, TaskWorkspaceBindingFilter)]
        self.assertTrue(filters, "TaskWorkspaceBindingFilter not in handler registry")
        # It must always return False so it never wakes an ordinary message.
        self.assertFalse(filters[0].filter(FakeEvent(), None))


if __name__ == "__main__":
    unittest.main()
