"""Explicit public export boundary; runtime and Git data are never exported."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIRECTORIES = ('core', 'plugins', 'baseline', 'tests', 'docs', 'tools', 'LICENSES')
FILES = (
    '.gitignore', 'VERSION', 'README.md', 'CHANGELOG.md', 'THIRD_PARTY_NOTICES.md',
    'PRIVACY.md', 'VALIDATION.md', 'deploy.py', 'build_release.py', 'run_tests.py',
    'compatible_hashes.json', 'core.patch', 'plugins.patch', 'release_hashes.json',
)


def release_files(root=ROOT):
    paths = [root / name for name in FILES]
    for directory in DIRECTORIES:
        paths.extend(path for path in (root / directory).rglob('*')
                     if path.is_file() and '__pycache__' not in path.parts
                     and path.suffix not in ('.pyc', '.pyo'))
    for path in sorted(paths):
        if not path.is_file() or path.is_symlink():
            raise ValueError(f'Missing or symbolic export file: {path.relative_to(root)}')
        yield path
