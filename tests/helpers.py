# -*- coding: utf-8 -*-
"""Shared fakes for task-workspace tests (no live AstrBot required)."""
from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

PLUGINS = Path(__file__).resolve().parents[1] / "plugins"
if str(PLUGINS) not in sys.path:
    sys.path.insert(0, str(PLUGINS))

from astrbot_plugin_task_workspace.config import TaskWorkspaceConfig  # noqa: E402
from astrbot_plugin_task_workspace.memory_gate import (  # noqa: E402
    CAPABILITY_FEATURE,
    CAPABILITY_KEY,
    MemoryGate,
)


class FakeEvent:
    def __init__(self, umo="aiocqhttp:private:10001", sender="10001", *,
                 admin=True, private=True, message=""):
        self.unified_msg_origin = umo
        self._sender = sender
        self._admin = admin
        self._private = private
        self.message_str = message
        self._extra = {}
        self._stopped = False

    def is_admin(self):
        return self._admin

    def is_private_chat(self):
        return self._private

    def get_sender_id(self):
        return self._sender

    def get_platform_id(self):
        return self.unified_msg_origin.split(":")[0]

    def get_message_str(self):
        return self.message_str

    def set_extra(self, key, value=None):
        self._extra[key] = value

    def get_extra(self, key=None, default=None):
        if key is None:
            return dict(self._extra)
        return self._extra.get(key, default)

    def plain_result(self, text):
        return ("plain", text)

    def stop_event(self):
        self._stopped = True

    def is_stopped(self):
        return self._stopped

    def should_call_llm(self, flag):
        pass


class FakeConversation:
    def __init__(self, cid, user_id, title="", persona_id="", history=None):
        self.cid = cid
        self.user_id = user_id
        self.title = title
        self.persona_id = persona_id
        self.history = json.dumps(history or [])


class FakeConversationManager:
    def __init__(self):
        self._current = {}
        self._convs = {}
        self._counter = 0
        # Mirror the real ConversationManager's synchronous live selection map.
        self.session_conversations = {}

    async def new_conversation(self, umo, platform_id=None, content=None,
                               title=None, persona_id=None):
        self._counter += 1
        cid = f"cid{self._counter:04d}"
        self._convs[cid] = FakeConversation(
            cid, umo, title or "", persona_id or "", content or []
        )
        self._current[umo] = cid
        self.session_conversations[umo] = cid
        return cid

    async def update_conversation(self, umo, conversation_id=None, history=None,
                                  title=None, persona_id=None, token_usage=None):
        cid = conversation_id or self._current.get(umo)
        conv = self._convs.get(cid)
        if conv is None:
            return
        if title is not None:
            conv.title = title
        if persona_id is not None:
            conv.persona_id = persona_id
        if history is not None:
            conv.history = json.dumps(history)

    async def get_curr_conversation_id(self, umo):
        return self.session_conversations.get(umo) or self._current.get(umo)

    async def switch_conversation(self, umo, conversation_id):
        if conversation_id not in self._convs:
            raise ValueError("no such conversation")
        self._current[umo] = conversation_id
        self.session_conversations[umo] = conversation_id

    async def get_conversation(self, umo, conversation_id, create_if_not_exists=False):
        return self._convs.get(conversation_id)

    async def get_conversations(self, umo=None, platform_id=None):
        return [c for c in self._convs.values() if umo is None or c.user_id == umo]

    async def append_history(self, cid, role, content):
        conv = self._convs[cid]
        hist = json.loads(conv.history)
        hist.append({"role": role, "content": content})
        conv.history = json.dumps(hist)


class FakeSp:
    def __init__(self):
        self._global = {}
        self._umo = {}

    def global_get(self, key, default=None):
        return self._global.get(key, default)

    def global_put(self, key, value):
        self._global[key] = value

    async def get_async(self, scope, scope_id, key, default=None):
        return self._umo.get((scope, scope_id, key), default)

    async def put_async(self, scope, scope_id, key, value):
        self._umo[(scope, scope_id, key)] = value

    async def session_get(self, umo, key, default=None):
        return self._umo.get(("umo", umo, key), default)

    async def session_put(self, umo, key, value):
        self._umo[("umo", umo, key)] = value

    async def session_remove(self, umo, key):
        self._umo.pop(("umo", umo, key), None)


class FakeContext:
    def __init__(self, cm=None):
        self.conversation_manager = cm or FakeConversationManager()
        self.sent = []

    async def send_message(self, session, chain):
        text = chain.get_plain_text() if hasattr(chain, "get_plain_text") else str(chain)
        self.sent.append((session, text))
        return True


def make_config(**overrides):
    cfg = {
        "enabled": True,
        "allowed_sessions": ["aiocqhttp:private:10001"],
        "require_admin_private": True,
        "max_global_runs": 2,
        "default_permission": "read-only",
        "default_timeout_s": 600,
        "default_executor": "dsh",
        "workspace_persona": "运维",
        "context_max_chars": 6000,
        "result_max_chars": 4000,
        "workspace_root": "D:\\agent-workspaces",
        "ssh": {"host": "192.0.2.10", "user": "bridgeuser", "work_dir": "D:\\agent-bridge"},
    }
    cfg.update(overrides)
    return TaskWorkspaceConfig(cfg)


def make_gate(capable=True, sp=None):
    from astrbot_plugin_task_workspace.memory_capability import (
        CAPABILITY_VERSION,
        MemoryCapabilityRegistry,
    )

    registry = MemoryCapabilityRegistry()
    gate = MemoryGate(sp=sp or FakeSp(), registry=registry)
    if capable:
        provider = FakeProvider(True)
        registry.publish(provider, CAPABILITY_VERSION)
        gate._test_provider = provider  # keep a strong ref for the weak registry
    return gate


@contextlib.contextmanager
def native_guard(version=None):
    """Install/remove the native follow-up guard marker for a test.

    ``version=None`` removes the marker (unpatched core); an int installs it on
    the *imported* ``astrbot.core.pipeline.process_stage.follow_up`` module, the
    only place ``native_guard_status`` accepts as evidence.
    """
    import astrbot.core.pipeline.process_stage.follow_up as fu
    from astrbot_plugin_task_workspace.binding import NATIVE_GUARD_MARKER

    had = hasattr(fu, NATIVE_GUARD_MARKER)
    old = getattr(fu, NATIVE_GUARD_MARKER, None)
    if version is None:
        if had:
            delattr(fu, NATIVE_GUARD_MARKER)
    else:
        setattr(fu, NATIVE_GUARD_MARKER, version)
    try:
        yield
    finally:
        if had:
            setattr(fu, NATIVE_GUARD_MARKER, old)
        elif hasattr(fu, NATIVE_GUARD_MARKER):
            delattr(fu, NATIVE_GUARD_MARKER)


class FakeProvider:
    """Minimal stand-in for a loaded LivingMemory instance."""

    def __init__(self, ready=True):
        self._terminating = False
        self._ready = ready

    def memory_isolation_ready(self):
        return self._ready
