# Public release privacy boundary

This repository is a sanitized source export. The live installation is not a Git working tree and is not included in the release.

Exported content is restricted by `tools/release_files.py`. Runtime configurations, chat histories, data directories, databases, credentials, SSH keys, logs, bytecode, backups, private deployment records and Git internals are excluded. Original public author/copyright attributions are retained; deployment account names, private machine paths and chat identifiers are removed from examples. Example identifiers are synthetic.

`tools/privacy_check.py` checks every exported text file for personal home paths, chat IDs, private keys, known token formats and credential URLs without printing matched values. SHA-256 source identities are retained for exact installation compatibility, not as reversible copies of private source text. Automated patterns cannot establish that arbitrary text is non-sensitive: inspect changes before tagging, and check for actual deployment secrets locally without recording them in reports.

Only sanitized files enter the initial Git commit; private content must never be committed and then removed in a later commit. Use a release-only Git identity without a personal email. Build archives from the explicit export list; never archive the live deployment or include `.git`. No credentials are embedded in repository URLs.

If a release contains private content, remove the public artifact, rotate any exposed credentials, and handle repository history separately; deleting a file in a new commit does not erase previous versions.

The configured runtime may still produce sensitive logs or private backups. This export policy applies to distributed source and archives, not to claims that runtime output is automatically redacted.
