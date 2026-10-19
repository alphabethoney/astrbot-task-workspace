import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

PACKAGE=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('dual_deploy',PACKAGE/'deploy.py')
deploy=importlib.util.module_from_spec(spec);spec.loader.exec_module(deploy)

class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)/'root';self.site=Path(self.tmp.name)/'site'
        for src,target,baseline in deploy.sources(self.root,self.site):
            if baseline.exists():
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(baseline,target)
        self.config=self.root/'data/config/astrbot_plugin_task_workspace_config.json'
        self.config.parent.mkdir(parents=True,exist_ok=True)
        self.original={'enabled':True,'allowed_sessions':['test:FriendMessage:admin'],'default_permission':'read-only','unrelated':'preserve'}
        self.config.write_text(json.dumps(self.original))
        self.db=self.root/'data/plugin_data/astrbot_plugin_task_workspace/task_workspace.db'
        self.db.parent.mkdir(parents=True,exist_ok=True)
        with sqlite3.connect(self.db) as conn:
            conn.execute('CREATE TABLE sentinel(value TEXT)');conn.execute("INSERT INTO sentinel VALUES('before')")

    def tearDown(self):self.tmp.cleanup()

    def test_apply_and_complete_rollback(self):
        deploy.preflight(self.root,self.site)
        with patch.object(deploy,'ensure_stopped'):
            deploy.apply(self.root,self.site)
        config=json.loads(self.config.read_text(encoding='utf-8-sig'))
        self.assertEqual(config['default_permission'],'workspace-write')
        self.assertEqual(config['unrelated'],'preserve')
        self.assertEqual(config['linux_cwd'], str(self.root))
        self.assertTrue((self.site/'astrbot/core/workspace_runtime.py').exists())
        with sqlite3.connect(self.db) as conn:conn.execute("UPDATE sentinel SET value='after'")
        backup=next((self.db.parent/'deploy_backups').iterdir())
        deploy.restore(backup)
        self.assertEqual(json.loads(self.config.read_text()),self.original)
        self.assertFalse((self.site/'astrbot/core/workspace_runtime.py').exists())
        with sqlite3.connect(self.db) as conn:self.assertEqual(conn.execute('SELECT value FROM sentinel').fetchone()[0],'before')
        deploy.preflight(self.root,self.site)

    def test_reject_newer_local_edits(self):
        target=self.site/'astrbot/core/astr_main_agent.py'
        target.write_text(target.read_text()+'\n# a newer user edit\n')
        with self.assertRaisesRegex(RuntimeError,'不一致'):deploy.preflight(self.root,self.site)

    def test_preserve_explicit_runtime_settings(self):
        data = {**self.original, 'linux_cwd': '/srv/custom-work', 'linux_max_runs': 1}
        self.config.write_text(json.dumps(data))
        with patch.object(deploy, 'ensure_stopped'):
            deploy.apply(self.root, self.site)
        config = json.loads(self.config.read_text(encoding='utf-8-sig'))
        self.assertEqual(config['linux_cwd'], '/srv/custom-work')
        self.assertEqual(config['linux_max_runs'], 1)
