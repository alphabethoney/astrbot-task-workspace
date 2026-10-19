#!/usr/bin/env python3
"""Install/roll back a matched core + plugin release while AstrBot is stopped."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time

PACKAGE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def atomic_write(path, data, mode=0o644):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.dual-window-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def ensure_stopped():
    result = subprocess.run(['systemctl', 'is-active', 'astrbot.service'], capture_output=True, text=True, timeout=10)
    if result.returncode not in (3, 4) or result.stdout.strip() not in ('inactive', 'failed', 'unknown'):
        raise RuntimeError('必须先在主机停止 astrbot.service；无法确认服务已停止时拒绝部署')


def sources(root, site):
    for directory, target_root in [('core', site), ('plugins', root / 'data/plugins')]:
        for src in sorted((PACKAGE / directory).rglob('*')):
            if not src.is_file() or '__pycache__' in src.parts or src.suffix == '.pyc':
                continue
            relative = src.relative_to(PACKAGE / directory)
            baseline = PACKAGE / 'baseline' / (relative if directory == 'core' else Path('plugins') / relative)
            yield src, target_root / relative, baseline


def preflight(root, site):
    manifest = json.loads((PACKAGE / 'release_hashes.json').read_text())
    for relative, expected in manifest.items():
        if digest(PACKAGE / relative) != expected:
            raise RuntimeError(f'发布包校验失败：{relative}')
    compatibility = json.loads((PACKAGE / 'compatible_hashes.json').read_text())
    for src, target, baseline in sources(root, site):
        if target.is_symlink():
            raise RuntimeError(f'目标为符号链接，拒绝替换：{target}')
        source_key = str(src.relative_to(PACKAGE))
        accepted = [digest(baseline), digest(src), *compatibility.get(source_key, [])]
        if digest(target) not in accepted:
            raise RuntimeError(f'线上文件与基线或本发布包不一致，请重新合并：{target}')
    config = root / 'data/config/astrbot_plugin_task_workspace_config.json'
    data = json.loads(config.read_text(encoding='utf-8-sig'))
    if not data.get('enabled') or not data.get('allowed_sessions'):
        raise RuntimeError('工作区必须已启用且配置明确的授权会话')
    if not data.get('require_admin_private', True):
        raise RuntimeError('本发布要求管理员私聊模式')
    return config, data


def backup_file(target, backup, entries):
    item = {'target': str(target), 'existed': target.exists()}
    if target.exists():
        name = f'files/{len(entries):04d}'
        dst = backup / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target, dst)
        item.update(saved=name, mode=target.stat().st_mode & 0o777)
    entries.append(item)


def restore(backup):
    manifest = json.loads((backup / 'backup.json').read_text())
    for item in manifest['files']:
        target = Path(item['target'])
        if item['existed']:
            atomic_write(target, (backup / item['saved']).read_bytes(), item['mode'])
        else:
            target.unlink(missing_ok=True)
    db = Path(manifest['database'])
    for suffix in ('-wal', '-shm'):
        Path(str(db) + suffix).unlink(missing_ok=True)
    if manifest['database_existed']:
        atomic_write(db, (backup / 'task_workspace.db').read_bytes(), manifest.get('database_mode', 0o600))
    else:
        db.unlink(missing_ok=True)
    clear_bytecode([Path(item['target']) for item in manifest['files']])


def clear_bytecode(paths):
    for path in paths:
        if path.suffix == '.py':
            for cache in (path.parent / '__pycache__').glob(path.stem + '.*.pyc'):
                cache.unlink(missing_ok=True)


def apply(root, site):
    ensure_stopped()
    config, data = preflight(root, site)
    files = list(sources(root, site))
    # Check access before touching any installed file.
    for _, target, _ in files:
        ancestor = target.parent
        while not ancestor.exists():
            ancestor = ancestor.parent
        if not os.access(ancestor, os.W_OK):
            raise PermissionError(f'目录不可写：{ancestor}')
    backup = root / 'data/plugin_data/astrbot_plugin_task_workspace/deploy_backups' / time.strftime('%Y%m%d-%H%M%S')
    backup.mkdir(parents=True, mode=0o700, exist_ok=False)
    entries = []
    for _, target, _ in files:
        backup_file(target, backup, entries)
    backup_file(config, backup, entries)
    database = root / 'data/plugin_data/astrbot_plugin_task_workspace/task_workspace.db'
    existed = database.exists()
    if existed:
        with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as original, sqlite3.connect(backup / 'task_workspace.db') as copied:
            original.backup(copied)
    manifest = {'files': entries, 'database': str(database), 'database_existed': existed,
                'database_mode': database.stat().st_mode & 0o777 if existed else 0o600}
    atomic_write(backup / 'backup.json', (json.dumps(manifest, indent=2) + '\n').encode(), 0o600)
    try:
        for src, target, _ in files:
            atomic_write(target, src.read_bytes(), target.stat().st_mode & 0o777 if target.exists() else 0o644)
        data.update(default_permission='workspace-write', require_native_guard=True)
        if not data.get('linux_cwd'):
            data['linux_cwd'] = str(root)
        data.setdefault('linux_max_runs', 2)
        # The Windows native-tool profile is always empty in code; Linux uses
        # its own explicit profile. Preserve other existing settings/secrets.
        atomic_write(config, ('\ufeff' + json.dumps(data, ensure_ascii=False, indent=2) + '\n').encode('utf-8'), config.stat().st_mode & 0o777)
        clear_bytecode([target for _, target, _ in files])
    except BaseException:
        restore(backup)
        raise
    print(f'安装完成。数据库迁移在下次 AstrBot 启动时执行。回滚备份：{backup}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('check', 'apply', 'rollback'))
    parser.add_argument('--root', type=Path, help='AstrBot deployment root (required for check/apply)')
    parser.add_argument('--site-packages', type=Path, help='AstrBot Python site-packages (required for check/apply)')
    parser.add_argument('--backup', type=Path)
    args = parser.parse_args()
    if args.action in ('check', 'apply') and (args.root is None or args.site_packages is None):
        parser.error('check/apply require --root and --site-packages; paths are never inferred')
    if args.action == 'check':
        preflight(args.root.resolve(), args.site_packages.resolve())
        print('发布包和线上基线校验通过；未修改文件或服务。')
    elif args.action == 'apply':
        apply(args.root.resolve(), args.site_packages.resolve())
    else:
        ensure_stopped()
        if args.backup is None:
            parser.error('rollback requires --backup')
        restore(args.backup.resolve())
        print('代码、配置和工作区数据库已恢复；请启动 AstrBot。')


if __name__ == '__main__':
    main()
