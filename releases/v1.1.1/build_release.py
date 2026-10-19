"""Regenerate review patches and checksums after editing the staged release."""
from pathlib import Path
import difflib
import hashlib
import json
import sys

ROOT=Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'tools'))
from release_files import release_files

def main():
    for folder in ['core','plugins']:
        changes=[]
        for path in sorted((ROOT/folder).rglob('*')):
            if not path.is_file() or '__pycache__' in path.parts or path.suffix=='.pyc':continue
            relative=path.relative_to(ROOT/folder)
            baseline=ROOT/'baseline'/(relative if folder=='core' else Path('plugins')/relative)
            old=baseline.read_text() if baseline.exists() else ''
            changes.extend(difflib.unified_diff(old.splitlines(True),path.read_text().splitlines(True),fromfile='a/'+str(relative) if baseline.exists() else '/dev/null',tofile='b/'+str(relative)))
        (ROOT/(folder+'.patch')).write_text(''.join(changes))
    manifest_path = ROOT / 'release_hashes.json'
    if not manifest_path.exists():
        manifest_path.write_text('{}\n')
    manifest={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest()
              for path in release_files() if path != manifest_path}
    (ROOT/'release_hashes.json').write_text(json.dumps(manifest,indent=2)+'\n')

if __name__=='__main__':main()
