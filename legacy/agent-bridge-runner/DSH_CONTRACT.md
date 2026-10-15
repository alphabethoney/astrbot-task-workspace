# Verified Windows DSH contract (supervisor research)

Installed CLI version 0.2.0-rc.2, executable ProductVersion 0.2.0.0. Source/schema extracted read-only from installed ASAR to <REVIEW_DIR>. No paid agent task or user config modification.

DSH_PERMISSION_MODE IS read by shipped base sandbox-policy, default workspace-write; approval defaults to ask unless danger-full-access. ENV ALONE therefore insufficient for unattended read-only/workspace-write with approval never.

Use explicit final CLI patch generated per fresh runner-owned execution, with all three entries (YAML shown; JSON patch-list is also possible only if schema loader supports it):

```yaml
- id: sandbox-policy
  config:
    mode: read-only
    workspaceRoot: 'D:\agent-workspaces\ws_...'
- id: approval
  config:
    policy: never
- id: permission
  config:
    defaultPreset: runner-read-only
    presets:
      runner-read-only:
        sandbox: read-only
        approval: never
```

For workspace-write substitute mode/preset consistently. Do not permit danger-full-access by default. Args: `--profile headless --patch ABSOLUTE_OVERLAY_PATH --json -`; send task stdin then close. DSH_PERMISSION_MODE may also be set consistently but patch is authoritative config layer. No --sandbox/--approval-policy invented flags.

THIRD entry is REQUIRED: permission-presets initialize rejects composed sandbox/approval combination not matching a preset; read-only+never and workspace-write+never need explicit custom preset/defaultPreset. Fresh session pins these values. never rejects approvals before any answerer (does not mean approve automatically).

Critical resume caveat: --session-id adopts persisted session; saved sandbox/mode and approval/policy override profile defaults. Reject arbitrary supplied session_id in this first runner version, or require verified runner provenance/policy. Prefer fresh sessions plus explicit task input snapshot; that matches workspace multiple-run design.

Evidence sources extracted:
- dsh-base/cordis.patch.yml:223-262 ENV defaults.
- dsh-permission-presets/lib/index.js:178-180 matching default preset;377-382 fresh pinned preset.
- dsh-sandbox-policy/lib/index.js:141-146 effective mode precedence.
- dsh-user-approval/lib/index.js:152-175 never policy and saved policy precedence.
- headless startup accepts --json --session-id task/help; stdin '-' supported.
- profile-schema.json patch list config whole replacement; IDs sandbox-policy/approval/permission.
- file sandbox read-only rejects write/edit; pwsh sandbox restricts non-full-access and propagates unavailable errors.

Limits: tool filesystem/command effects are constrained, not every internal DSH persistence write; Windows restricted-token backend is documented partial. No claim of total process isolation. User-composed mounted plugin composition was not dumped, avoiding credential exposure; real acceptance must verify requested mode visible and tool permission behavior in scratch workspace. Preserved session IDs must not bypass the configured policy.
