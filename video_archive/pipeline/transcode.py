"""Lossless remux: move the moov atom to the front (ffmpeg -c copy -movflags +faststart).

No re-encode, so this is cheap and bit-identical video/audio; only the container
layout changes. Used to fix up videos uploaded before faststart was enforced.
"""
import subprocess

from video_archive.core import media_tools, sha256
from video_archive.pipeline.common import PipelineError

TIMEOUT = 1800


def remux_faststart(src, dst, *, check=lambda: None):
    media_tools()
    check()
    try:
        proc = subprocess.run(
            # -f mp4 forced explicitly: ffmpeg otherwise guesses the muxer from
            # dst's filename extension, and scratch filenames do not have one.
            ['ffmpeg', '-nostdin', '-v', 'error', '-y', '-i', str(src),
             '-c', 'copy', '-movflags', '+faststart', '-f', 'mp4', str(dst)],
            capture_output=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        raise PipelineError('transcode_timeout') from None
    check()
    if proc.returncode or not dst.is_file() or dst.stat().st_size == 0:
        raise PipelineError('transcode_failed')
    return sha256(dst)
