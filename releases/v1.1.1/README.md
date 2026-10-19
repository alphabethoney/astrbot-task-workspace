# AstrBot dual task windows — 1.1.1

Sanitized source distribution for Linux native and Windows remote task windows. This is a matched core/plugin upgrade for a compatible deployment, not a universal plugin ZIP.

## Behavior

```text
/task new linux Local maintenance
/task new windows Remote development
/task list
/task switch <workspace-id>
/task info
/task status
/task result <execution-id>
/task cancel <execution-id>
```

Linux windows use an explicit local tool set and the AstrBot service account's permissions. Windows execution uses `/task run dsh` or `/task run codex` and the existing SSH runner. Switching windows preserves execution ownership. New remote submissions use `workspace-write`.

This does not provide an OS sandbox. Ordinary chat retains its existing behavior. Consultation-only mode, `/chat list`, unified CID policies and cache optimization are future work documented in [the roadmap](docs/cid-context-and-cache-plan.md).

## Contents and compatibility

- `plugins/`: task workspace, bridge server and progress plugins, version 1.1.1.
- `core/`: matching AstrBot core modifications; `baseline/` and patches support review.
- `tests/`: isolated tests, with simulated model and SSH providers.
- `compatible_hashes.json`: SHA-256 identities for previously validated source revisions; contains no original private text.

The validated AstrBot base is 4.27.5 with earlier task-workspace integrations. The installer checks exact file identities and rejects unrelated changes. Matching version numbers alone are insufficient. A loaded LivingMemory isolation integration is required; it and the Windows runner are not included. Existing deployments using legacy handoff must preserve that configuration and state.

## Verify and install

Use the Python interpreter of the target AstrBot environment. Substitute actual deployment and site-packages paths; `/srv/astrbot` and `/opt/astrbot/...` below are examples.

```bash
python tools/privacy_check.py
python run_tests.py
python deploy.py check --root /srv/astrbot --site-packages /opt/astrbot/lib/python3.13/site-packages
sudo systemctl stop astrbot.service
python deploy.py apply --root /srv/astrbot --site-packages /opt/astrbot/lib/python3.13/site-packages
sudo systemctl start astrbot.service
```

Resolve active executions before stopping. Installation requires stopped `astrbot.service`, explicit enabled/admin-private session configuration and a matching source baseline. It backs up affected files, configuration and the workspace database privately, preserves configured Linux cwd/limits, and prints the backup path. Never commit those backups. Schema migration occurs at startup.

If verification fails, investigate before modifying the baseline. Do not bypass baseline verification or install only the three plugins without their matching core changes.

## Validation and rollback

After startup verify all three plugin versions, schema v3, legacy workspace environments and live LivingMemory isolation capability. In authorized windows test a harmless Linux command, background switching and cancellation, then a Windows task and result routing. Automated tests do not prove real model/provider or SSH compatibility.

```bash
sudo systemctl stop astrbot.service
python deploy.py rollback --backup /path/to/private/backup
sudo systemctl start astrbot.service
```

Rollback restores the pre-upgrade workspace database; newer task records will be lost. Coordinate a maintenance window.

## Versioning and packaging

Use Conventional Commits. Runtime changes bump plugin metadata and `VERSION` together; roadmap items must not be described as shipped features. Release tags are immutable. Review changes, run tests, regenerate hashes with `python build_release.py`, run the privacy check, and create the archive with `python tools/package_release.py`. SHA-256 sidecars detect changes but are not cryptographic signatures.

Only the approved source/documentation trees are packaged. Runtime configuration, identities, credentials, databases, logs, private deployment records and bytecode are excluded. See [privacy policy](PRIVACY.md) and [third-party notices](THIRD_PARTY_NOTICES.md).
