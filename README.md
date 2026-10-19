# AstrBot Task Workspace

## Current release: v1.1.1

The current matched Linux/Windows core and plugin distribution is in
[`releases/v1.1.1/`](releases/v1.1.1/README.md). Download the source archive and
SHA-256 sidecar from [GitHub Releases](https://github.com/alphabethoney/astrbot-task-workspace/releases/tag/v1.1.1).

- [Version changes](releases/v1.1.1/CHANGELOG.md)
- [CID, consultation windows and context/cache roadmap](releases/v1.1.1/docs/cid-context-and-cache-plan.md) — planned features, not implemented by v1.1.1.
- [Privacy boundary](releases/v1.1.1/PRIVACY.md) and [validation](releases/v1.1.1/VALIDATION.md).

The root `plugins/`, `tests/`, `config/`, and `legacy/` directories below are the
previous implementation and supporting references, retained without rewriting
history. Do not combine the old root plugin with the v1.1.1 core. Follow the
release directory's installer and exact compatibility checks. Existing Windows
runner and LivingMemory patch references remain available under `legacy/`;
the source archive does not bundle or automatically install them.

## Previous implementation reference


Source snapshot for a persistent-session task workspace on top of AstrBot,
plus the runner, bridge, and memory-isolation patches it depends on. This tree
is a reviewable code package, **not** a turnkey deployment: configuration,
credentials, runtime data, and rollout remain the operator's responsibility.

```
chat -> AstrBot (task-workspace plugin, Linux host)
          |  SSH (paramiko)
          v
        bridge runner (Windows)  ->  dsh / codex adapters
          |  child process
          v
        CLI agent
```

## Components

| Path | Role |
| --- | --- |
| `plugins/astrbot_plugin_task_workspace/` | Independent plugin: `/task` management, SQLite run ledger, LLM result surfacing, memory-isolation gate, bridge-compatible execution adapter. |
| `legacy/astrbot_plugin_bridge_server/` | Bridge server plugin: SSH dispatch, task table, `/wd` dashboard, compatibility routing into the task-workspace service. |
| `legacy/agent-bridge-runner/` | Windows runner: receives tasks over stdin, drives CLI agents, writes durable `events.jsonl`, and adds request-id dedupe + `reconcile`. |
| `legacy/astrbot_core_patch/` | Native AstrBot `follow_up.py` binding-guard patch (synchronous capture guard). |
| `legacy/astrbot_plugin_livingmemory_patch/` | LivingMemory memory-isolation hook patch. |
| `config/` | Generic configuration examples. |
| `tests/` | Stdlib `unittest` suite (real AstrBot imports plus fakes). |

The plugin is fail-closed: `enabled` defaults to `false` and `allowed_sessions`
defaults to empty (nobody is authorised). Both must be set explicitly before any
command works. Default execution permission is `read-only`.

## Use

Managed workspaces are admin-private conversations. A workspace is a persistent
AstrBot conversation; the current workspace is derived from the current
conversation id, never from a competing pointer.

1. `/task new <title>` — create a conversation and select it (create does not run).
2. Discuss requirements in that conversation.
3. `/task run` (or `/task run dsh`) — snapshot the conversation and submit to the
   background executor.
4. `/task new <title>` again for a second workspace; discussions and runs stay
   isolated.
5. `/task list`, `/task switch <workspace-id>`, `/task status`, `/task result`.
6. `/task amend <text>` records a requirement for the *next* run (never injected
   into a running one).
7. `/task cancel`, `/task reconcile`, `/task archive`, `/task restore`.

Run ids and workspace ids differ; use the ids returned by `/task list`.

### Runner and bridge

- The runner reads task JSON from stdin (never spliced into a command line),
  forces UTF-8, and appends events by `seq` so `watch` is resumable.
- The bridge (`use_task_workspace: true`) routes legacy commands
  `/wg /wc /ww /wl /wx /nt /switch /tasks /close /rename` to the task-workspace
  service. If the flag is on but the service is absent it returns an explicit
  error; it never falls back to the legacy path.
- Legacy submissions carry a unique `request_id`; the runner dedupes on it and
  exposes `reconcile`.

### Runner policy

`legacy/agent-bridge-runner/policy.json` is an example policy. It uses
environment placeholders for executables (`${DSH_CLI}`, `${CODEX_CLI}`, expanded
by the runner via `os.path.expandvars`), a bounded sample workspace root
(`D:\agent-workspaces`), and a `workspace-write` permission ceiling (no
`danger-full-access`). The DSH agent must declare a **verified** three-entry
patch — `sandbox-policy`, `approval: never`, and an explicit
`permission` preset — because the shipped base config defaults to
`workspace-write`/`ask`, which is unsafe unattended. Unverified or env-only DSH
permission configuration is rejected for every mode.

## Configuration

Copy `config/task_workspace_config.example.json` and
`config/bridge_config.example.json`, replace the placeholders
(`<REMOTE_RUNNER_DIR>`, `<KNOWN_HOSTS_PATH>`), and set a real allowed session.
The examples use the documentation IP `192.0.2.10` and a non-production account
id sample; they contain no credentials. SSH host-key policy defaults to
`reject`; use a configured `known_hosts` and disable plaintext passwords.

## Patches

Neither patch is applied to a live tree by this repository. Apply only after
review, on a copy, and let the operator own rollout.

- **Core follow-up guard** (`legacy/astrbot_core_patch/`): copy
  `follow_up_guard.py` beside the installed
  `astrbot/core/pipeline/process_stage/follow_up.py`, then
  from the directory containing the `astrbot` package run
  `patch -p0 --dry-run < <PATCH_DIR>/follow_up.py.patch`, then apply with `-p0`. The guard is evaluated synchronously right
  before native capture (no `await` in between) so a managed run cannot capture
  text belonging to another workspace.
- **LivingMemory isolation** (`legacy/astrbot_plugin_livingmemory_patch/`): copy
  `task_workspace_isolation.py` into `core/`, then apply the four diffs
  (`main.py`, `core/tools/memory_search_tool.py`,
  `core/tools/memory_memorize_tool.py`, `core/passive_group_capture.py`). Verify
  the base hash of your LivingMemory checkout first; the diffs target
  v2.6.0-beta.3 plus site-specific changes.

Ordinary daily memory and native follow-up behaviour are unchanged; only events
explicitly stamped by the task-workspace plugin (or belonging to a managed
workspace) are affected.

## Migration from the legacy bridge

`import_migration.LegacyImporter` dry-runs the legacy bridge task table,
verifies conversation ownership (the record-supplied owner is never trusted),
adopts active remote jobs by task id without resubmitting, and quarantines
unresolved rows individually. `handoff.py` records a versioned, read-back
verified handoff marker and refuses to hand off while legacy rows are active.
Use `require_legacy_handoff: true` when migrating so a workspace-first boot
cannot execute before the bridge publishes readiness.

## Tests

Tests need an interpreter that can `import astrbot` (AstrBot 4.27.x); no package
installation is performed.

```sh
ASTRBOT_PYTHON=/path/to/astrbot/python ./run_tests.sh
# or, if an importing interpreter is on PATH:
./run_tests.sh
```

`run_tests.sh` probes for an importing interpreter when `ASTRBOT_PYTHON` is
unset, sets an isolated `TASK_WORKSPACE_DATA_DIR`, and runs
`python -m unittest discover -s tests -t tests`. Tests use fakes and temp
directories; they make no network calls and touch no live data.

The patch-validity test recovers a pristine baseline reversibly when the running
core is already patched, so core correctness is still exercised on patched and
unpatched cores alike.

## Deployment boundaries

- This tree contains only source: no databases, runtime task data, logs,
  credentials, user histories, backups, or caches.
- The publishing parent owns git and any network publication; this snapshot does
  not create remotes, push, or run agents.
- The Windows sandbox is a tool-level constraint, not a VM or OS-level tenant
  boundary.
- Real end-to-end QQ receive/send acceptance, restart notification delivery, and
  live Codex runs are operator responsibilities and are not asserted here.

## Licensing

The bridge server and runner READMEs state MIT terms (`LICENSE` files were not
present in the source snapshot and are not invented here). The task-workspace
plugin carries no license file in this snapshot; treat that as an open gap and
resolve it before external redistribution. No upstream AstrBot or LivingMemory
source is vendored: the patches are diffs against operator-supplied trees, so
their licenses are inherited from those upstream projects.
