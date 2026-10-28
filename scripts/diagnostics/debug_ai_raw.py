"""One-off debug: call the AI provider once for a single video and print the
raw model response before JSON parsing, to see why json_JSONDecodeError
happened (pipeline_ai.py's call_model swallows the raw content on failure).

Read-only: does not touch Lease/claim/finish, so it never changes the task's
status or attempts count in the database.

Usage:
    .venv/bin/python scripts/diagnostics/debug_ai_raw.py <video_id>
"""
import argparse
import base64
import json
import sys
from pathlib import Path
import urllib.request

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from video_archive.pipeline.runner import ai_input
from video_archive.pipeline.ai import extract_frames, system_prompt, strip_fence
from video_archive.pipeline.cache import Cache
from video_archive.pipeline.common import BASE, ai_config, config
from video_archive.pipeline.db import Database
from video_archive.pipeline.storage import Storage


def raw_call(frames, cfg):
    content = [{'type': 'text', 'text': '以下是同一个视频按时间顺序抽取的画面帧，请按系统提示的要求输出JSON。'}]
    for frame in frames:
        content.append({'type': 'image_url', 'image_url': {
            'url': 'data:image/jpeg;base64,' + base64.b64encode(frame).decode('ascii')}})
    body = json.dumps({'model': cfg['model'], 'temperature': 0.7, 'messages': [
        {'role': 'system', 'content': system_prompt()}, {'role': 'user', 'content': content}]}).encode()
    request = urllib.request.Request(
        cfg['base_url'] + '/chat/completions', data=body, method='POST',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg['api_key']})
    with urllib.request.urlopen(request, timeout=cfg['request_timeout']) as response:
        return json.loads(response.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('video_id')
    parser.add_argument('--data', type=str, default=str(BASE / 'archive'))
    args = parser.parse_args()

    c = config()
    db = Database(c['database_dsn'])
    storage = Storage(c)
    cache = Cache(args.data, project_ref=c['project_ref'])
    cfg = ai_config()

    with ai_input(db, storage, cache, args.video_id, lambda: None) as path:
        frames = extract_frames(path, count=cfg.get('frame_count', 6), max_dim=cfg.get('max_dim', 768))
        payload = raw_call(frames, cfg)

    choice = payload['choices'][0]
    message = choice.get('message', {})
    print('finish_reason:', choice.get('finish_reason'))
    other = {k: v for k, v in payload.items() if k != 'choices'}
    if other:
        print('other top-level fields:', json.dumps(other, ensure_ascii=False, indent=2))

    content = message.get('content') or ''
    print('--- raw content (%d chars) ---' % len(content))
    print(content)
    print('--- end raw content ---')

    try:
        json.loads(strip_fence(content))
        print('\n[OK] parses as valid JSON after strip_fence.')
    except json.JSONDecodeError as exc:
        print(f'\n[FAIL] still not valid JSON after strip_fence: {exc}')


if __name__ == '__main__':
    sys.exit(main())
