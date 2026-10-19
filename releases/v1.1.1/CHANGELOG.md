# Changelog

## 1.1.1

- Prepare a sanitized source distribution, separate from the live deployment.
- Remove deployment-specific home paths and numeric chat identifiers from source examples and documentation.
- Require explicit deployment root and Python site-packages paths for installation checks and application.
- Preserve configured Linux working directory and concurrency limits; use the deployment root when the directory is unset.
- Use the service working directory for an unconfigured Linux window directory.
- Add version tracking, reproducible source archive tooling, privacy checks, file hashes, and the CID/context/cache roadmap with Kakobot design references.
- No consultation-only profile, `/chat list`, unified CID policies, or cache optimization is implemented by this version; those remain roadmap items.

## 1.1.0

- Separate Linux native and Windows remote task windows.
- Freeze execution ownership, validate tool dispatch, and scope process operations and cancellation.
- Add schema v3 local execution records, restart interruption handling, and environment-labelled progress.
- Reject textual pseudo-tool calls as unexecuted data and guard reloads during active local execution.

The live deployment and the public source release have independent rollout states. Publishing 1.1.1 does not upgrade an existing 1.1.0 installation.
