"""Frame sampling and the multimodal model call for the second pipeline stage.

build_provider() reads .private/ai.json (same permission model as
.private/pipeline.json, but a separate file since the model key has its own
lifecycle). call_model() talks to an OpenAI-chat-completions-compatible
endpoint (base_url + '/chat/completions'); frames go in as base64 JPEG
image_url parts alongside the system prompt in prompts/prompt.md.
"""
import base64
import contextvars
import json
import os
import re
import subprocess
import tempfile
import urllib.request
from datetime import datetime
from pathlib import Path

from video_archive.core import media_tools
from video_archive.pipeline.common import BASE, PipelineError, ai_config

# Set by run_ai so a usage line can name the drama without logging the response body.
usage_subject = contextvars.ContextVar('usage_subject', default=None)

_whisper_models = {}


def probe_duration(path):
    media_tools()
    result = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                             '-of', 'json', str(path)], capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise PipelineError('ffprobe_failed')
    duration = json.loads(result.stdout).get('format', {}).get('duration')
    if not duration or float(duration) <= 0:
        raise PipelineError('duration_unavailable')
    return float(duration)


def extract_frames(path, count=6, max_dim=768, timeout=120):
    """Evenly spaced JPEG frames, margins avoid title/credit cards at start and end."""
    media_tools()
    if not 1 <= count <= 20 or not 64 <= max_dim <= 2048:
        raise PipelineError('invalid_frame_sampling')
    duration = probe_duration(path)
    margin = duration * 0.05
    span = max(duration - 2 * margin, 0.1)
    frames = []
    for i in range(count):
        timestamp = margin + span * (i + 0.5) / count
        proc = subprocess.run(
            ['ffmpeg', '-nostdin', '-v', 'error', '-ss', f'{timestamp:.3f}', '-i', str(path),
             '-frames:v', '1', '-vf', f"scale='min({max_dim},iw)':-2",
             '-q:v', '3', '-f', 'image2pipe', '-vcodec', 'mjpeg', '-'],
            capture_output=True, timeout=timeout)
        if proc.returncode or not proc.stdout:
            raise PipelineError('frame_extraction_failed')
        frames.append(proc.stdout)
    return frames


def probe_has_audio(path):
    media_tools()
    result = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'a',
                             '-show_entries', 'stream=index', '-of', 'json', str(path)],
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise PipelineError('ffprobe_failed')
    return bool(json.loads(result.stdout).get('streams'))


def _whisper_model(name):
    model = _whisper_models.get(name)
    if model is None:
        from faster_whisper import WhisperModel
        model = _whisper_models[name] = WhisperModel(name, device='cpu', compute_type='int8')
    return model


def transcribe(path, model_name, check=lambda: None):
    """Local ASR over the video's own audio track; '' if it has no audio at all."""
    media_tools()
    if not probe_has_audio(path):
        return ''
    fd, tmp_name = tempfile.mkstemp(suffix='.wav')
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        proc = subprocess.run(
            ['ffmpeg', '-nostdin', '-v', 'error', '-y', '-i', str(path), '-vn',
             '-ac', '1', '-ar', '16000', '-f', 'wav', str(tmp_path)],
            capture_output=True, timeout=180)
        if proc.returncode or tmp_path.stat().st_size == 0:
            raise PipelineError('audio_extraction_failed')
        check()
        segments, _ = _whisper_model(model_name).transcribe(str(tmp_path), language='zh')
        text = ''.join(segment.text for segment in segments).strip()
        check()
        return text
    finally:
        tmp_path.unlink(missing_ok=True)


def system_prompt():
    path = BASE / 'prompts/prompt.md'
    text = path.read_text().strip() if path.is_file() else ''
    if not text:
        raise PipelineError('ai_system_prompt_missing')
    return text


def strip_fence(text):
    match = re.fullmatch(r'```(?:json)?\s*(.*?)\s*```', text.strip(), re.S)
    return match.group(1) if match else text.strip()


def record_model_usage(payload):
    """Append provider usage and cost. The key and the message body are not written."""
    usage = payload.get('usage')
    generation_id = payload.get('id')
    if usage is None and not generation_id:
        return
    path = BASE / 'archive' / 'ai-usage.jsonl'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    row = {
        'at': datetime.now().astimezone().isoformat(),
        **(usage_subject.get() or {}),
        'id': generation_id,
        'model': payload.get('model'),
        'usage': usage,
    }
    with path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + '\n')


def call_model(frames, transcript, cfg, check):
    """One OpenAI-chat-completions-compatible request; never logs the key or raw response."""
    content = [{'type': 'text', 'text': '以下是同一个视频按时间顺序抽取的画面帧，请按系统提示的要求输出JSON。'}]
    for frame in frames:
        content.append({'type': 'image_url', 'image_url': {
            'url': 'data:image/jpeg;base64,' + base64.b64encode(frame).decode('ascii')}})
    if transcript:
        content.append({'type': 'text', 'text':
            '以下是这一集完整的语音转写文字（本地自动转写，可能有错别字或漏字，但覆盖了全程台词）：\n' + transcript})
    body = json.dumps({'model': cfg['model'], 'temperature': 0.7, 'messages': [
        {'role': 'system', 'content': system_prompt()}, {'role': 'user', 'content': content}]}).encode()
    request = urllib.request.Request(
        cfg['base_url'] + '/chat/completions', data=body, method='POST',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg['api_key']})
    check()
    with urllib.request.urlopen(request, timeout=cfg['request_timeout']) as response:
        payload = json.loads(response.read())
    check()
    record_model_usage(payload)
    return json.loads(strip_fence(payload['choices'][0]['message']['content']))


class Provider:
    is_test_double = False

    def __init__(self, cfg):
        self.cfg = cfg

    def analyze(self, path, check=lambda: None):
        check()
        frames = extract_frames(path, count=self.cfg.get('frame_count', 6),
                                 max_dim=self.cfg.get('max_dim', 768))
        check()
        transcript = transcribe(path, self.cfg.get('whisper_model', 'small'), check)
        check()
        return call_model(frames, transcript, self.cfg, check)


def build_provider(path=None):
    cfg = ai_config(path) if path is not None else ai_config()
    return Provider(cfg)
