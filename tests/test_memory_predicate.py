# -*- coding: utf-8 -*-
"""Shared memory-suppression predicate semantics (item 4).

The predicate must inspect the frozen binding first, then fall back to the same
live policy the early filter used. It must never fail open for a potentially
managed workspace.
"""
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[1] / "legacy" / "astrbot_plugin_livingmemory_patch"))

from helpers import FakeEvent  # noqa: E402

import task_workspace_isolation as iso  # noqa: E402


class FakePolicy:
    def __init__(self, *, managed_umos=(), managed_cids=(), current=None,
                 raise_current=False):
        self._umos = set(managed_umos)
        self._cids = set(managed_cids)
        self._current = dict(current or {})
        self._raise = raise_current

    def current_cid(self, umo):
        if self._raise:
            raise RuntimeError("policy exploded")
        return self._current.get(umo, "")

    def is_managed_cid(self, cid):
        return bool(cid) and cid in self._cids

    def is_managed_umo(self, umo):
        return umo in self._umos


class Ctx:
    pass


def _ctx_with(policy):
    ctx = Ctx()
    setattr(ctx, iso.POLICY_CONTEXT_ATTR, policy)
    return ctx


def _event(umo="aiocqhttp:private:10001", binding=None):
    ev = FakeEvent(umo=umo)
    if binding is not None:
        ev.set_extra(iso.BINDING_EXTRA_KEY, binding)
    return ev


def _managed_binding(umo="aiocqhttp:private:10001"):
    return {"umo": umo, "cid": "c_m", "ws_id": "ws_x", "owner": "10001",
            "managed": True}


def _ordinary_binding(umo="aiocqhttp:private:10001"):
    return {"umo": umo, "cid": "c_o", "ws_id": "", "owner": "", "managed": False}


class PredicateTests(unittest.TestCase):
    def test_frozen_managed_binding_denies(self):
        ctx = _ctx_with(FakePolicy())
        self.assertTrue(iso.policy_denies_capture(ctx, _event(binding=_managed_binding())))

    def test_frozen_ordinary_binding_allows_independent_of_selection(self):
        # Policy still reports the UMO's managed cid, but the frozen ordinary
        # binding was verified: daily memory must be allowed.
        ctx = _ctx_with(FakePolicy(managed_umos=["aiocqhttp:private:10001"],
                                   managed_cids=["c_m"],
                                   current={"aiocqhttp:private:10001": "c_m"}))
        self.assertFalse(
            iso.policy_denies_capture(ctx, _event(binding=_ordinary_binding())))

    def test_no_binding_managed_cid_denies_regardless_of_owner(self):
        ctx = _ctx_with(FakePolicy(managed_cids=["c_m"],
                                   current={"aiocqhttp:private:10001": "c_m"}))
        self.assertTrue(iso.policy_denies_capture(ctx, _event()))

    def test_no_binding_validated_ordinary_cid_allows(self):
        ctx = _ctx_with(FakePolicy(managed_umos=["aiocqhttp:private:10001"],
                                   managed_cids=["c_m"],
                                   current={"aiocqhttp:private:10001": "c_daily"}))
        self.assertFalse(iso.policy_denies_capture(ctx, _event()))

    def test_no_binding_missing_selection_known_managed_denies(self):
        ctx = _ctx_with(FakePolicy(managed_umos=["aiocqhttp:private:10001"]))
        self.assertTrue(iso.policy_denies_capture(ctx, _event()))

    def test_no_policy_at_all_is_ordinary(self):
        self.assertFalse(iso.policy_denies_capture(Ctx(), _event()))

    def test_filter_order_reversed_still_denies_managed(self):
        # is_isolated() is False (stamp not yet applied) but the frozen managed
        # binding still denies, independent of handler priority.
        ev = _event(binding=_managed_binding())
        self.assertFalse(iso.is_isolated(ev))
        ctx = _ctx_with(FakePolicy())
        self.assertTrue(iso.policy_denies_capture(ctx, ev))

    def test_managed_after_switch_to_ordinary_stays_denied(self):
        ctx = _ctx_with(FakePolicy(current={"aiocqhttp:private:10001": "c_daily"}))
        self.assertTrue(
            iso.policy_denies_capture(ctx, _event(binding=_managed_binding())))

    def test_exception_path_denies(self):
        ctx = _ctx_with(FakePolicy(raise_current=True))
        self.assertTrue(iso.policy_denies_capture(ctx, _event()))

    def test_broken_event_extra_denies_when_policy_managed(self):
        class BadEvent:
            unified_msg_origin = "aiocqhttp:private:10001"

            def get_extra(self, *a, **k):
                raise RuntimeError("no extras")

        ctx = _ctx_with(FakePolicy(managed_cids=["c_m"],
                                   current={"aiocqhttp:private:10001": "c_m"}))
        # binding lookup fails -> falls to policy -> managed cid -> deny
        self.assertTrue(iso.policy_denies_capture(ctx, BadEvent()))


class ToolPatchTests(unittest.TestCase):
    def test_memory_tool_patches_use_shared_predicate(self):
        root = HERE.parents[1] / "legacy" / "astrbot_plugin_livingmemory_patch"
        for name in ("memory_search_tool.py.patch", "memory_memorize_tool.py.patch"):
            text = (root / name).read_text(encoding="utf-8")
            self.assertIn("policy_denies_capture", text)
            self.assertIn("getattr(context.context, \"context\", None)", text)


if __name__ == "__main__":
    unittest.main()
