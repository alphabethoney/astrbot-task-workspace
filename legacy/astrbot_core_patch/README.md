# Staged AstrBot core patch: native follow-up binding guard

**Do not apply this to a live AstrBot core tree.** Deployment is a separate,
approved step handled by the parent (including version hash / safe rollout).

## Problem

`astrbot.core.pipeline.process_stage.follow_up.try_capture_follow_up` runs at
the very start of the internal agent sub-stage, *before* any plugin
`on_waiting_llm_request` hook, and selects the active run by
`unified_msg_origin` + sender id only. A plugin cannot pre-empt it. Even an
early adapter-message filter (see `astrbot_plugin_task_workspace.binding`) has a
TOCTOU window: a run can register during asynchronous preprocessing *after* the
filter ran.

## Patch contents

* `follow_up_guard.py` — a standalone, side-effect-free comparator.
  `binding_guard_allows(incoming_event, runner_event)` returns `False` unless
  the two events are safe to pair.
* `follow_up.py.patch` — a minimal unified diff that:
  1. imports `binding_guard_allows` from `.follow_up_guard`;
  2. defines `TASK_WORKSPACE_BINDING_GUARD_VERSION = 1` (readiness marker);
  3. calls the guard immediately before `runner.follow_up(...)`, with **no
     `await` in between**, returning `None` (no capture) on mismatch.

Apply (review first, in a copy):

```sh
# <SITE_PACKAGES> is the directory containing the `astrbot` package
# (for example the venv/site-packages used to run AstrBot).
CORE=<SITE_PACKAGES>
cp legacy/astrbot_core_patch/follow_up_guard.py \
   "$CORE/astrbot/core/pipeline/process_stage/follow_up_guard.py"
cd "$CORE"
patch -p0 --dry-run < <PATCH_DIR>/follow_up.py.patch
patch -p0 < <PATCH_DIR>/follow_up.py.patch
```

## Semantics

| incoming | runner | result |
| --- | --- | --- |
| ordinary | ordinary | allow (native behaviour unchanged) |
| managed | managed, same umo/owner/cid/ws | allow |
| managed | managed, different | deny |
| managed | missing/ordinary | deny |
| missing/ordinary | managed | deny |

The guard reads only the conventional event extra
`_task_workspace_binding`; it does not import the task-workspace plugin, so an
unpatched/unloaded plugin cannot be affected, and ordinary daily memory is
untouched.

`tests/test_follow_up_guard.py` exercises the comparator, its integration with
the real `_ACTIVE_AGENT_RUNNERS` registry, and verifies the patch applies
cleanly to a throwaway copy of the installed module.
