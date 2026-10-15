# livingmemory memory-isolation patch (staged, NOT deployed)

Target: `<ASTRBOT_ROOT>/data/plugins/astrbot_plugin_livingmemory`
(`main.py` + `core/tools/*.py` + `core/passive_group_capture.py`).

**Do not apply this to a live tree.** Deployment is a separate, approved step
handled by the parent.

## What the patch does (minimal)

1. Adds `core/task_workspace_isolation.py`:
   - `is_isolated(event)` — returns True when the event extra
     `task_workspace_memory_isolated` is set.
   - `advertise(sp)` — retained compatibility **no-op** (the old persisted
     flag was never authoritative, and `SharedPreferences.global_get/put` are
     async-only, so the old synchronous calls warned). Safe to keep calling.
   - `publish_live(context, provider)` / `revoke_live(context, provider)` —
     register/remove the *loaded instance* in the task-workspace in-process
     capability registry attached to the shared `Context`. `publish_live` logs
     live readiness (success/failure + `capability_version`) through
     `astrbot.api.logger`; no secrets.
   - `policy_denies_capture(context, event)` — consults the same synchronous
     managed-workspace policy index the task-workspace early filter uses, so
     isolation is independent of handler priority.
2. `main.py`:
   - publishes the live capability **only after** runtime components and hook
     guards load successfully (`_initialize_plugin` -> `_ensure_runtime_components`);
   - adds `memory_isolation_ready()` (live readiness marker);
   - revokes on `terminate()`;
   - early-returns from `handle_memory_recall` / `handle_memory_reflection` when
     the event is isolated **or** the managed policy denies it.
3. `core/tools/memory_search_tool.py` / `memory_memorize_tool.py`: early-return
   an empty result when the event is isolated (agent tools are not an
   authorization boundary; the gate must hold regardless).
4. `core/passive_group_capture.py`: skip capture when the event is isolated or
   the managed policy denies it.

This changes no existing default behaviour: only events explicitly stamped by
the task-workspace plugin (or belonging to a managed workspace) are affected;
ordinary daily memory is untouched.

## Capability contract

* The task-workspace `MemoryGate` trusts only the **live in-process registry**,
  never the persisted `livingmemory.capabilities` flag.
* A provider is available only while it is weakly reachable, not terminating,
  and `memory_isolation_ready()` returns `True`. Unload/downgrade revokes
  immediately.
* `memory_isolation_extra = 1`. A future breaking change must bump the version.

## Apply (review first)

```sh
LM=<ASTRBOT_ROOT>/data/plugins/astrbot_plugin_livingmemory
P=<PATCH_DIR>/astrbot_plugin_livingmemory_patch
cp "$P/task_workspace_isolation.py" "$LM/core/task_workspace_isolation.py"
cd "$LM"
patch -p1 --dry-run < "$P/main.py.patch"
patch -p1 < "$P/main.py.patch"
patch -p1 < "$P/memory_search_tool.py.patch"
patch -p1 < "$P/memory_memorize_tool.py.patch"
patch -p1 < "$P/passive_group_capture.py.patch"
```

The `main.py.patch` and `passive_group_capture.py.patch` diffs are generated
against a livingmemory tree at v2.6.0-beta.3 plus site-specific changes; the
operator must verify the base hash of their own checkout before applying.

There is intentionally no `apply.sh` that touches the live tree.

## Initial scope

Managed workspaces are admin-private conversations. Group capture that would
involve managed workspaces is explicitly denied via `policy_denies_capture`;
the general passive group capture path for ordinary groups is unchanged.
