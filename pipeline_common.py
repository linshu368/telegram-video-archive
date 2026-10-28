"""Private configuration, stable identities and read-only legacy import."""
import json
import os
import re
import sqlite3
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
NAMESPACE = uuid.UUID('795ec7d2-3505-4cef-9f46-8aad88c7028b')


class PipelineError(Exception):
    pass


def error_code(exc):
    # Never log exception messages from drivers, HTTP responses or providers.
    if isinstance(exc, PipelineError) and re.fullmatch(r'[a-z0-9_]{1,80}', str(exc)):
        return str(exc)
    kind = type(exc)
    # Module-qualified class name only: tells sqlite3 from psycopg without any message text.
    return (kind.__module__.split('.')[0] + '_' + kind.__name__)[:80]


def text(value):
    return value.strip() or None if isinstance(value, str) else None


def tags(value):
    return list(dict.fromkeys(x.strip() for x in (value or []) if isinstance(x, str) and x.strip()))


def ai_output(value):
    if not isinstance(value, dict) or set(value) != {'title', 'tags', 'description'}:
        raise PipelineError('invalid_ai_output')
    if any(value[k] is not None and not isinstance(value[k], str) for k in ('title', 'description')):
        raise PipelineError('invalid_ai_output')
    if not isinstance(value['tags'], list) or any(not isinstance(x, str) for x in value['tags']):
        raise PipelineError('invalid_ai_output')
    if len(value['tags']) > 100 or any(len(x) > 200 for x in value['tags']) or any(
            len(value[k] or '') > limit for k, limit in [('title', 500), ('description', 20000)]):
        raise PipelineError('invalid_ai_output')
    return {'title': text(value['title']), 'tags': tags(value['tags']), 'description': text(value['description'])}


def display(video, ai=None):
    ai = ai or {}
    return {'title': text(video.get('original_title')) or text(ai.get('title')),
            'tags': tags(video.get('original_tags')) or tags(ai.get('tags')),
            'description': text(video.get('original_description')) or text(ai.get('description')),
            'episode': text(video.get('episode'))}


def identity(kind, source, item):
    return str(uuid.uuid5(NAMESPACE, f'telegram:{int(source)}:{kind}:{int(item)}'))


def object_path(vid):
    return f'videos/{uuid.UUID(str(vid))}/original'


def cover_object_path(vid):
    # Same video id as the original: the cover needs no identity of its own.
    return f'covers/{uuid.UUID(str(vid))}/original.jpg'


def ai_config(path=BASE / '.private/ai.json'):
    """Separate, equally-locked-down file: the model key has its own lifecycle, not Supabase's."""
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise PipelineError('private_ai_config_missing')
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077 or path.parent.stat().st_mode & 0o077:
        raise PipelineError('private_config_permissions')
    c = {'frame_count': 6, 'max_dim': 768, 'request_timeout': 120, 'whisper_model': 'small',
         **json.loads(path.read_text())}
    if any(k not in c or c[k] in ('', None) for k in ('provider', 'model', 'api_key', 'base_url')):
        raise PipelineError('ai_config_incomplete')
    if type(c['frame_count']) is not int or not 1 <= c['frame_count'] <= 20:
        raise PipelineError('invalid_frame_sampling')
    if type(c['max_dim']) is not int or not 64 <= c['max_dim'] <= 2048:
        raise PipelineError('invalid_frame_sampling')
    if type(c['request_timeout']) is not int or not 10 <= c['request_timeout'] <= 600:
        raise PipelineError('invalid_ai_request_timeout')
    if c['whisper_model'] not in ('tiny', 'base', 'small', 'medium', 'large-v2', 'large-v3'):
        raise PipelineError('invalid_whisper_model')
    c['base_url'] = c['base_url'].rstrip('/')
    if not re.fullmatch(r'https://[A-Za-z0-9.-]+(?::\d+)?(?:/[^\s]*)?', c['base_url']):
        raise PipelineError('invalid_ai_base_url')
    return c


def config(path=BASE / '.private/pipeline.json'):
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise PipelineError('private_pipeline_config_missing')
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077 or path.parent.stat().st_mode & 0o077:
        raise PipelineError('private_config_permissions')
    c = json.loads(path.read_text())
    required = ('project_ref', 'region', 'plan', 'database_dsn', 'storage_key',
                'global_file_limit', 'bucket_file_limit', 'sample_budget_confirmed')
    if any(k not in c or c[k] in ('', None) for k in required):
        raise PipelineError('pipeline_config_incomplete')
    if not re.fullmatch(r'[a-z0-9]{20}', c['project_ref']):
        raise PipelineError('invalid_project_ref')
    if c['plan'] not in ('free', 'pro', 'team', 'enterprise'):
        raise PipelineError('invalid_plan')
    if not (type(c['global_file_limit']) is int and type(c['bucket_file_limit']) is int
            and 0 < c['bucket_file_limit'] <= c['global_file_limit']):
        raise PipelineError('invalid_file_limits')
    if c['plan'] == 'free' and c['global_file_limit'] > 50 * 1024 * 1024:
        raise PipelineError('free_plan_limit')
    # Direct TLS connection only; credentials never passed through command arguments.
    from psycopg.conninfo import conninfo_to_dict
    info = conninfo_to_dict(c['database_dsn'])
    if (any(k in info for k in ('hostaddr', 'service', 'options'))
            or any(os.environ.get(k) for k in ('PGHOSTADDR', 'PGSERVICE', 'PGOPTIONS'))):
        raise PipelineError('database_connection_override_forbidden')
    if info.get('sslmode') not in ('verify-full', 'verify-ca', 'require'):
        raise PipelineError('database_tls_required')
    ref, host = c['project_ref'], info.get('host') or ''
    # Direct host, or the official Supavisor session pooler (IPv4) whose login role names the project.
    direct = host == f'db.{ref}.supabase.co'
    pooled = (re.fullmatch(r'aws-\d+-[a-z0-9-]+\.pooler\.supabase\.com', host) is not None
              and str(info.get('port', '5432')) == '5432'
              and (info.get('user') or '').endswith('.' + ref))
    if not (direct or pooled):
        raise PipelineError('database_project_mismatch')
    if c.get('sample_budget_confirmed') is not True:
        raise PipelineError('sample_budget_not_confirmed')
    # Explicit administrator evidence; never describe it as remotely discovered.
    try:
        verified = datetime.fromisoformat(c['dashboard_verified_at'])
        age = (datetime.now(timezone.utc) - verified).total_seconds()
        if not 0 <= age <= 7 * 86400:
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise PipelineError('recent_dashboard_confirmation_required') from None
    if not text(c.get('sample_budget_note')):
        raise PipelineError('sample_budget_scope_required')
    return c


def cover_scope(project_ref):
    """Validated key for per-project cover evidence; never defaulted."""
    if not isinstance(project_ref, str) or not re.fullmatch(r'[a-z0-9]{20}', project_ref):
        raise PipelineError('cover_project_ref_required')
    return project_ref


def cover_remote(state, project_ref):
    """What this cloud project has committed for a cover; {} if nothing yet.

    The local cover file is shared by every project, but whether a cover is
    uploaded is a fact about one Supabase project: a test project's readback
    must never count as proof for the main one.
    """
    return (state.get('remote') or {}).get(cover_scope(project_ref), {})


def mark_cover_remote(state, project_ref, **values):
    state.setdefault('remote', {}).setdefault(cover_scope(project_ref), {}).update(values)
    return state


def covers_committed(root, project_ref):
    """Message ids whose cover track is terminal for ONE cloud project.

    Terminal means that project's remote cover was read back (remote_sha256) or
    Telegram's lack of a thumbnail was committed to its database. Anything else
    stays selectable so a failed cover is retried even once its video is archived.
    """
    scope = cover_scope(project_ref)
    path = Path(root).resolve() / 'state.sqlite3'
    if not path.exists():
        return set()
    db = sqlite3.connect(f'{path.as_uri()}?mode=ro', uri=True)
    try:
        rows = db.execute('SELECT message_id,data FROM covers').fetchall()
    except sqlite3.OperationalError:
        return set()  # Archive predates covers; every video is still uncovered.
    finally:
        db.close()
    done = set()
    for mid, raw in rows:
        c = json.loads(raw)
        remote = (c.get('remote') or {}).get(scope, {})
        if ((c.get('status') == 'success' and remote.get('remote_sha256'))
                or (c.get('status') == 'unavailable' and remote.get('note_committed'))
                or c.get('status') == 'skipped'):
            done.add(int(mid))
    return done


def snapshot(root, topics_path):
    """Read one consistent SQLite snapshot. Never open legacy Store to import."""
    root = Path(root).resolve()
    topics = json.loads(Path(topics_path).read_text())
    names = {}
    for t in topics:
        if t.get('deleted') or not text(t.get('topic_title')):
            continue
        key = (int(t['source_id']), int(t['topic_id']))
        if key in names and names[key] != text(t['topic_title']):
            raise PipelineError('topic_catalog_conflict')
        names[key] = text(t['topic_title'])
    db = sqlite3.connect(f'{(root / "state.sqlite3").as_uri()}?mode=ro', uri=True)
    result, excluded = [], Counter()
    try:
        db.execute('BEGIN')
        for raw, state in db.execute('SELECT m.data,v.data FROM messages m JOIN videos v ON m.id=v.message_id ORDER BY m.id'):
            r, s = json.loads(raw), json.loads(state)
            sid, mid, tid = int(r['source']['id']), int(r['message_id']), r.get('topic_id')
            if tid == 1:
                excluded['general'] += 1
                continue
            if not tid or (sid, tid) not in names:
                excluded['missing_topic'] += 1
                continue
            if not r.get('media_id'):
                excluded['missing_media'] += 1
                continue
            did, vid = identity('topic', sid, tid), identity('message', sid, mid)
            d = {'id': did, 'title': names[sid, tid], 'source_id': str(sid), 'topic_id': str(tid)}
            v = {'id': vid, 'drama_id': did, 'source_id': str(sid), 'message_id': str(mid),
                 'media_id': str(r['media_id']), 'original_title': text(r.get('title')),
                 'original_tags': tags(r.get('tags')), 'original_description': text(r.get('description')),
                 'episode': text(r.get('episode'))}
            result.append((d, v, r, s))
    finally:
        db.close()
    return result, dict(excluded)
