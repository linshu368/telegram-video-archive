"""LLM structuring of channel text: one drama-level record plus a per-video episode.

Runs after quality screening and before import_video, so only accepted dramas
cost a call. Any failure leaves the regex-derived fields from snapshot() as is.
"""
import hashlib
import json
import urllib.request
from pathlib import Path

from video_archive.core import atomic_json
from video_archive.pipeline.ai import record_model_usage, strip_fence
from video_archive.pipeline.common import BASE, PipelineError, error_code, tags, text

PROMPT_PATH = BASE / 'prompts/text_structuring.md'
MAX_BUNDLE_CHARS = 60_000
INVALID = 'invalid_text_structure'


def system_prompt():
    body = PROMPT_PATH.read_text().strip() if PROMPT_PATH.is_file() else ''
    if not body:
        raise PipelineError('text_structuring_prompt_missing')
    return body


def structure_cache_file(root, project_ref):
    return Path(root) / 'pipeline' / 'projects' / project_ref / 'text-structure-cache.json'


def build_bundle(hint, members):
    messages = sorted(({'message_id': record['message_id'], 'text': (record.get('text') or '').strip()}
                       for _, _, record, _ in members), key=lambda m: m['message_id'])
    return {'drama_hint': text(hint), 'messages': messages}


def cache_key(bundle, model):
    blob = json.dumps({'bundle': bundle, 'model': model,
                       'prompt': hashlib.sha256(system_prompt().encode()).hexdigest()},
                      sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode()).hexdigest()


def validate_structure(bundle, value):
    """Strict shape check; returns the normalized drama record and per-message episodes."""
    if not isinstance(value, dict) or set(value) != {'drama', 'videos'}:
        raise PipelineError(INVALID)
    drama, videos = value['drama'], value['videos']
    if not isinstance(drama, dict) or set(drama) != {'title', 'description', 'tags'}:
        raise PipelineError(INVALID)
    for key, limit in (('title', 500), ('description', 20000)):
        if drama[key] is not None and (not isinstance(drama[key], str) or len(drama[key]) > limit):
            raise PipelineError(INVALID)
    if (not isinstance(drama['tags'], list) or len(drama['tags']) > 100
            or any(not isinstance(x, str) or len(x) > 200 for x in drama['tags'])):
        raise PipelineError(INVALID)
    if (not isinstance(videos, list)
            or any(not isinstance(x, dict) or set(x) != {'message_id', 'episode'} for x in videos)
            or any(type(x['message_id']) is not int for x in videos)):
        raise PipelineError(INVALID)
    expected = sorted(m['message_id'] for m in bundle['messages'])
    if sorted(x['message_id'] for x in videos) != expected:
        raise PipelineError(INVALID)
    numbers = [x['episode'] for x in videos if x['episode'] is not None]
    if (any(type(n) is not int or not 1 <= n <= 999999 for n in numbers)
            or len(numbers) != len(set(numbers))):
        raise PipelineError(INVALID)
    return {'title': text(drama['title']), 'description': text(drama['description']),
            'tags': tags(drama['tags']), 'episodes': {x['message_id']: x['episode'] for x in videos}}


def call_structuring(bundle, cfg, check):
    """One OpenAI-chat-completions-compatible text request; never logs the key or the response body."""
    body = json.dumps({'model': cfg.get('text_model') or cfg['model'], 'temperature': 0, 'messages': [
        {'role': 'system', 'content': system_prompt()},
        {'role': 'user', 'content': json.dumps(bundle, ensure_ascii=False)}]}).encode()
    request = urllib.request.Request(
        cfg['base_url'] + '/chat/completions', data=body, method='POST',
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg['api_key']})
    check()
    with urllib.request.urlopen(request, timeout=cfg['request_timeout']) as response:
        payload = json.loads(response.read())
    check()
    record_model_usage(payload)
    try:
        return json.loads(strip_fence(payload['choices'][0]['message']['content']))
    except (ValueError, KeyError, IndexError, TypeError):
        raise PipelineError(INVALID) from None


def structure_drama(bundle, cfg, check=lambda: None, complete=call_structuring):
    """Retry once on malformed output; transport errors propagate."""
    for attempt in range(2):
        try:
            return validate_structure(bundle, complete(bundle, cfg, check))
        except PipelineError as exc:
            if str(exc) != INVALID or attempt:
                raise


def _load_cache(path):
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _store(result):
    return {**result, 'episodes': {str(mid): ep for mid, ep in result['episodes'].items()}}


def _restore(entry):
    return {**entry, 'episodes': {int(mid): ep for mid, ep in entry['episodes'].items()}}


def structure_jobs(jobs, *, cfg, cache_path, skip_message_ids=(), check=lambda: None,
                   complete=call_structuring):
    """Overwrite original_title/description/tags/episode on each job's video dict, per drama.

    Dramas whose every member is already imported are skipped (import_video never
    rewrites those columns). Returns one report entry per drama.
    """
    groups = {}
    for job in jobs:
        groups.setdefault(job[1]['drama_id'], []).append(job)
    model = cfg.get('text_model') or cfg['model']
    cache = _load_cache(cache_path)
    skip, report = set(skip_message_ids), []
    for did, members in groups.items():
        entry = {'drama_id': did, 'messages': len(members)}
        report.append(entry)
        if all(job[2]['message_id'] in skip for job in members):
            entry['result'] = 'skipped_already_imported'
            continue
        bundle = build_bundle(members[0][0]['title'], members)
        if len(json.dumps(bundle, ensure_ascii=False)) > MAX_BUNDLE_CHARS:
            entry.update(result='fallback_regex', error='text_bundle_too_large')
            continue
        key = cache_key(bundle, model)
        if key in cache:
            result, entry['result'] = _restore(cache[key]), 'cached'
        else:
            try:
                result = structure_drama(bundle, cfg, check, complete)
            except Exception as exc:
                entry.update(result='fallback_regex', error=error_code(exc))
                continue
            cache[key] = _store(result)
            Path(cache_path).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            atomic_json(cache_path, cache)
            entry['result'] = 'structured'
        for _, video, record, _ in members:
            episode = result['episodes'][record['message_id']]
            video.update(original_title=result['title'], original_description=result['description'],
                         original_tags=list(result['tags']),
                         episode=str(episode) if episode is not None else None)
    return report
