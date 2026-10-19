"""Run isolated tests against staged core, using the installed AstrBot dependencies."""
from pathlib import Path
import importlib.abc
import importlib.util
import os
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent

class StagedCore(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        candidate = ROOT / 'core' / (fullname.replace('.', '/') + '.py')
        if candidate.is_file():
            return importlib.util.spec_from_file_location(fullname, candidate)
        return None

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='astrbot-dual-tests-') as sandbox:
        os.environ['ASTRBOT_ROOT'] = sandbox
        os.chdir(sandbox)
        sys.meta_path.insert(0, StagedCore())
        sys.path.insert(0, str(ROOT / 'plugins'))
        suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'))
        outcome = unittest.TextTestRunner(verbosity=2).run(suite)
        sys.exit(not outcome.wasSuccessful())
