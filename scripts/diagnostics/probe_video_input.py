"""One-off probe: does this AI gateway accept raw video bytes in a chat
completions request, instead of pre-extracted JPEG frames?

Trims the source video to a short, downscaled clip, sends it as a base64
data URI in a 'video_url' content part (mirroring the 'image_url' convention
pipeline_ai.py already uses for frames), and prints whatever the gateway
says back -- a clean 4xx about the content type is a clear "no"; a normal
completion that actually describes motion/audio the frames alone can't see
is a "yes, worth pursuing"; a completion that reads like it ignored the
video is inconclusive and needs a second opinion from you.

Read-only: does not touch Lease/claim/finish, no pipeline/db state changes.

Usage:
    .venv/bin/python scripts/diagnostics/probe_video_input.py <path-to-video> [--seconds 12] [--width 480]
"""
import argparse
import base64
import json
import subprocess
import sys
from pathlib import Path
import urllib.error
import urllib.request

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from video_archive.core import media_tools
from video_archive.pipeline.common import ai_config


def make_clip(path, seconds, width):
    media_tools()
    proc = subprocess.run(
        ['ffmpeg', '-nostdin', '-v', 'error', '-i', str(path), '-t', str(seconds),
         '-vf', f"scale='min({width},iw)':-2", '-c:v', 'libx264', '-crf', '30',
         '-preset', 'veryfast', '-c:a', 'aac', '-b:a', '64k',
         '-movflags', 'frag_keyframe+empty_moov', '-f', 'mp4', 'pipe:1'],
        capture_output=True, timeout=120)
    if proc.returncode or not proc.stdout:
        print('ffmpeg stderr:', proc.stderr.decode(errors='replace'), file=sys.stderr)
        raise SystemExit('clip_extraction_failed')
    return proc.stdout


def probe(clip_bytes, cfg):
    data_uri = 'data:video/mp4;base64,' + base64.b64encode(clip_bytes).decode('ascii')
    content = [
        {'type': 'text', 'text': '这是一段短视频片段。请按时间顺序描述发生了什么，'
                                  '包括你听到的任何对话内容。如果你无法处理视频内容，'
                                  '请直接说明你收到的是什么格式的输入。'},
        {'type': 'video_url', 'video_url': {'url': data_uri}},
    ]
    body = json.dumps({'model': cfg['model'], 'temperature': 0.2,
                        'messages': [{'role': 'user', 'content': content}]}).encode()
    request = urllib.request.Request(
        cfg['base_url'] + '/chat/completions', data=body, method='POST',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg['api_key']})
    try:
        with urllib.request.urlopen(request, timeout=cfg['request_timeout']) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors='replace')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('video_path')
    parser.add_argument('--seconds', type=int, default=12)
    parser.add_argument('--width', type=int, default=480)
    args = parser.parse_args()

    cfg = ai_config()
    clip = make_clip(args.video_path, args.seconds, args.width)
    print(f'clip size: {len(clip) / 1024:.0f} KiB raw, '
          f'{len(clip) * 4 / 3 / 1024:.0f} KiB base64 (approx)')

    status, payload = probe(clip, cfg)
    print('HTTP status:', status)
    if isinstance(payload, str):
        print('--- error body ---')
        print(payload)
        return

    choice = payload['choices'][0]
    print('finish_reason:', choice.get('finish_reason'))
    content = choice.get('message', {}).get('content') or ''
    print(f'--- content ({len(content)} chars) ---')
    print(content)


if __name__ == '__main__':
    sys.exit(main())
