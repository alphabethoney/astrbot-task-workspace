# Validation

This source release is based on the tested 1.1.0 dual-window implementation. Version 1.1.1 changes distribution privacy and installation portability; it does not implement the CID/cache roadmap.

Validation uses an isolated temporary directory, the compatible AstrBot Python environment, and mock model/SSH providers. Tests exercise local subprocess ownership and cancellation but do not send chat messages or execute Windows jobs. Installation/rollback tests use temporary copies and databases.

The release gate requires:

- the full suite in `run_tests.py` to pass;
- preservation of explicit cwd/concurrency settings and exact baseline rejection;
- a clean export privacy scan and file-manifest validation;
- reproducible archive bytes and no runtime/private files in archive entries.

Real provider tool calls, QQ delivery, and Windows runner connectivity require deployment-specific acceptance testing. The source release is not itself evidence that these external integrations work.

## Recorded results (2026-10-19)

- Full isolated suite: 28 tests passed, including the new explicit cwd/concurrency preservation test.
- Export privacy scan: 109 files, zero findings. A separate local comparison against recognized deployment-sensitive values found no matches; values are not included in this report.
- Read-only installer preflight against the validated 1.1.0 installation passed; production was not upgraded to 1.1.1.
- Distribution excludes production configuration, deployment history, databases, logs, backups and bytecode. The package gate verifies the exact export file set and hashes before creating an archive.
