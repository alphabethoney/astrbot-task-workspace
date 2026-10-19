"""Check public export content without printing potentially secret matches."""
import re
import sys

from release_files import ROOT, release_files

PATTERNS = {
    'personal_home': re.compile(r'/(?:home|Users)/[A-Za-z0-9_.-]+/'),
    'windows_profile': re.compile(r'[A-Za-z]:[\\/]+Users[\\/]+[A-Za-z0-9_.-]+'),
    'real_chat_id': re.compile(r'(?:FriendMessage|GroupMessage):[0-9]{6,}'),
    'private_key': re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'provider_key': re.compile(r'\b(?:sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b'),
    'credential_url': re.compile(r'https?://[^\s/:@]+:[^\s/@]+@'),
    'jwt': re.compile(r'\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\b'),
}
FORBIDDEN_PARTS = {'data', 'deploy_backups', 'private', '.git', '__pycache__'}
FORBIDDEN_SUFFIXES = {'.db', '.sqlite', '.sqlite3', '.log', '.pyc', '.pyo', '.pem', '.key'}


def scan(paths):
    findings = []
    for path in paths:
        relative = path.relative_to(ROOT)
        if set(relative.parts) & FORBIDDEN_PARTS or path.suffix in FORBIDDEN_SUFFIXES:
            findings.append((str(relative), 0, 'runtime_or_private_file'))
        try:
            content = path.read_text(encoding='utf-8')
        except UnicodeError:
            findings.append((str(relative), 0, 'unexpected_binary'))
            continue
        for line_no, line in enumerate(content.splitlines(), 1):
            for label, pattern in PATTERNS.items():
                if pattern.search(line):
                    findings.append((str(relative), line_no, label))
    return findings


if __name__ == '__main__':
    paths = list(release_files())
    findings = scan(paths)
    for path, line_no, label in findings:
        print(f'{path}:{line_no}: {label}')
    print(f'Privacy scan: {len(paths)} files, {len(findings)} findings (values suppressed).')
    sys.exit(bool(findings))
