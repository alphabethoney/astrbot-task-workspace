"""Build a deterministic, allowlisted source archive after integrity/privacy checks."""
import gzip
import hashlib
import io
import json
import tarfile

from privacy_check import scan
from release_files import ROOT, release_files


def main():
    paths = list(release_files())
    findings = scan(paths)
    if findings:
        raise SystemExit('Privacy check failed; run tools/privacy_check.py for locations.')
    expected = json.loads((ROOT / 'release_hashes.json').read_text())
    actual = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in paths if p.name != 'release_hashes.json'}
    if actual != expected:
        raise SystemExit('Release manifest mismatch; review and run build_release.py.')
    version = (ROOT / 'VERSION').read_text().strip()
    import re
    if not re.fullmatch(r'\d+\.\d+\.\d+', version):
        raise SystemExit('Invalid release version')
    prefix = f'astrbot-dual-windows-{version}'
    output = ROOT / 'dist'
    output.mkdir(exist_ok=True)
    archive = output / f'{prefix}.tar.gz'
    with archive.open('wb') as raw, gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as tar:
            for path in paths:
                content = path.read_bytes()
                info = tarfile.TarInfo(prefix + '/' + str(path.relative_to(ROOT)))
                info.size = len(content)
                info.mode = 0o644
                info.mtime = 0
                info.uid = info.gid = 0
                info.uname = info.gname = ''
                tar.addfile(info, io.BytesIO(content))
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_suffix(archive.suffix + '.sha256').write_text(f'{checksum}  {archive.name}\n')
    print(f'Created dist/{archive.name}; {len(paths)} allowlisted files; SHA-256 {checksum}')


if __name__ == '__main__':
    main()
