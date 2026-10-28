"""Save reproducible OFFLINE results separately from Telegram acceptance."""
import json
import platform
import subprocess
import sys
from pathlib import Path

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from video_archive.paths import BASE
from video_archive.core import atomic_json, now


def main():
    root = BASE
    output = root / 'archive'
    output.mkdir(exist_ok=True)
    proc = subprocess.run([sys.executable, '-m', 'unittest', 'discover',
                           '-s', str(root / 'tests'), '-v'],
                          cwd=root, capture_output=True, text=True)
    log = proc.stdout + proc.stderr
    (output / 'offline-tests.txt').write_text(log)
    atomic_json(output / 'implementation-validation.json', {
        'generated_at': now(), 'kind': 'OFFLINE_TESTS_NOT_TELEGRAM_ACCEPTANCE',
        'python': platform.python_version(), 'test_exit_code': proc.returncode,
        'passed': proc.returncode == 0,
        'test_log': str(output / 'offline-tests.txt'),
        'scope': 'Mock Telegram responses and actual generated video files; no account access.',
        'telegram_acceptance_report': str(output / 'report.json'),
    })
    saved = json.loads((output / 'implementation-validation.json').read_text())
    print(log)
    print('Saved offline report:', output / 'implementation-validation.json')
    return saved['test_exit_code']


if __name__ == '__main__':
    raise SystemExit(main())