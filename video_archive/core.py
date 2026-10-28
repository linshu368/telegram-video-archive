"""Local archive format, transaction boundaries and media validation. No network."""
import csv
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from telethon import types


SCHEMA_VERSION = 1
VIDEO_EXTS = {'.mp4', '.m4v', '.mov', '.mkv', '.webm', '.avi', '.ts', '.mts',
              '.m2ts', '.mpg', '.mpeg', '.3gp', '.flv', '.wmv', '.ogv', '.vob'}
SECRET_KEYS = {'access_hash', 'file_reference', 'auth_key', 'api_hash', 'phone',
               'phone_number', 'session', 'bytes', 'stripped_thumb', 'reply_markup'}
RESERVE = 512 * 1024 * 1024
JPEG_MAGIC = b'\xff\xd8\xff'
# A cover is Telegram's own thumbnail only: never an ffmpeg frame, never video bytes.
COVER_STATE = {'status': 'pending', 'attempts': 0, 'local_path': '', 'size': None,
               'sha256': '', 'thumb': None, 'downloaded_at': None, 'verified_at': None,
               'failure_reason': '', 'events': []}


class StopArchive(Exception):
    """Persist progress and require a later explicit resume."""


def now():
    return datetime.now(timezone.utc).isoformat()


def clean(value):
    if hasattr(value, 'to_dict'):
        value = value.to_dict()
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items() if k not in SECRET_KEYS}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value if not isinstance(v, bytes)]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return None
    return value


def atomic_json(path, data):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def disk_check(path, needed=0):
    free = shutil.disk_usage(path).free
    if free < needed + RESERVE:
        raise StopArchive(f'disk_insufficient: free={free}, required={needed}, '
                          f'reserve={RESERVE}, deficit={needed + RESERVE - free} bytes')
    return free


def document_of(message):
    media = getattr(message, 'media', None)
    # Webpage previews and paid previews are NOT downloadable attached videos.
    return getattr(media, 'document', None) if isinstance(media, types.MessageMediaDocument) else None


def classify(message):
    doc = document_of(message)
    if not isinstance(doc, types.Document):
        return [], 'no_direct_document'
    attrs = doc.attributes or []
    filename = next((a.file_name for a in attrs if isinstance(a, types.DocumentAttributeFilename)), '')
    evidence = []
    if any(isinstance(a, types.DocumentAttributeVideo) for a in attrs):
        evidence.append('DocumentAttributeVideo')
    if (doc.mime_type or '').lower().startswith('video/'):
        evidence.append('video_mime')
    if Path(filename).suffix.lower() in VIDEO_EXTS:
        evidence.append('video_extension_candidate')
    return evidence, '' if evidence else 'document_without_video_evidence'


def thumb_sizes(doc):
    """Downloadable thumbnail sizes only, with their index in doc.thumbs.

    PhotoStrippedSize is an inline blur placeholder, not a fetchable object, and
    PhotoCachedSize/PhotoSizeEmpty carry no retrievable file either.
    """
    result = []
    for index, thumb in enumerate(getattr(doc, 'thumbs', None) or []):
        if isinstance(thumb, types.PhotoSize):
            size = thumb.size
            progressive = False
        elif isinstance(thumb, types.PhotoSizeProgressive):
            # Progressive scan sizes are advisory: only the last one is the whole file.
            size, progressive = (max(thumb.sizes) if thumb.sizes else None), True
        else:
            continue
        result.append({'index': index, 'type': thumb.type, 'width': thumb.w,
                       'height': thumb.h, 'size': size, 'progressive': progressive})
    return result


def pick_thumb(doc):
    """The largest fetchable thumbnail, or None when Telegram supplied none."""
    candidates = thumb_sizes(doc)
    if not candidates:
        return None
    return max(candidates, key=lambda t: ((t['width'] or 0) * (t['height'] or 0), t['size'] or 0))


def extract_text(text):
    # Only explicitly labelled values. Never infer a drama identity from a caption.
    def labelled(label):
        m = re.search(rf'(?m)^[ \t]*(?:{label})[ \t]*[:：][ \t]*([^\r\n]+?)[ \t]*$', text)
        return m.group(1) if m else ''
    return {'title': labelled('标题|剧名'), 'description': labelled('简介'),
            'episode': labelled('集数'),
            'tags': list(dict.fromkeys(re.findall(r'(?<!\w)#([\w]+)', text)))}


def topic_membership(message, source):
    """Conservative forum identity from a full message, never from its caption."""
    raw = clean(message)
    if not isinstance(raw, dict) or raw.get('_') not in ('Message', 'MessageService'):
        return None, 'message_unavailable_or_incomplete'
    if (raw.get('action') or {}).get('_') == 'MessageActionTopicCreate':
        return (raw.get('id'), 'topic_creation_service') if source.get('forum') else (
            None, 'source_not_confirmed_forum')
    reply = raw.get('reply_to')
    if reply and reply.get('_') != 'MessageReplyHeader':
        return None, 'unsupported_reply_header'
    if reply and reply.get('forum_topic'):
        topic = reply.get('reply_to_top_id') or reply.get('reply_to_msg_id')
        if not isinstance(topic, int) or topic <= 0:
            return None, 'forum_header_missing_topic_id'
        other = reply.get('reply_to_peer_id')
        if other and other != raw.get('peer_id'):
            return None, 'cross_peer_reply_ambiguous'
        return topic, 'forum_reply_top_id' if reply.get('reply_to_top_id') else 'forum_reply_root_id'
    if not source.get('forum'):
        return None, 'source_not_confirmed_forum'
    # A non-forum thread's top ID is NOT a forum topic ID. General permits threads.
    if raw.get('_') == 'Message' and 'reply_to' in raw:
        return 1, 'general_full_message_without_forum_header'
    return None, 'service_or_missing_fields_without_topic_evidence'


def record_message(message, source):
    evidence, reason = classify(message)
    doc = document_of(message)
    attrs = getattr(doc, 'attributes', []) or []
    v = next((a for a in attrs if isinstance(a, types.DocumentAttributeVideo)), None)
    text = getattr(message, 'message', None) or ''
    reply = getattr(message, 'reply_to', None)
    topic_id, topic_reason = topic_membership(message, source)
    mid = message.id
    username = source['username']
    link = f'https://t.me/{username}/{mid}' if username else (
        f'https://t.me/c/{source["bare_id"]}/{mid}' if source['type'] != 'basic_group' else '')
    result = {
        'schema_version': SCHEMA_VERSION, 'platform': 'telegram', 'source': source,
        'message_id': mid, 'message_link': link,
        'published_at': clean(getattr(message, 'date', None)),
        'edited_at': clean(getattr(message, 'edit_date', None)),
        'text': text, 'text_entities': clean(getattr(message, 'entities', None) or []),
        **extract_text(text),
        'original_filename': next((a.file_name for a in attrs if isinstance(a, types.DocumentAttributeFilename)), ''),
        'media_id': getattr(doc, 'id', None), 'mime_type': getattr(doc, 'mime_type', '') or '',
        'expected_size': getattr(doc, 'size', None),
        'thumbs': thumb_sizes(doc),
        'duration_seconds': getattr(v, 'duration', None), 'width': getattr(v, 'w', None),
        'height': getattr(v, 'h', None), 'album_id': getattr(message, 'grouped_id', None),
        'reply': clean(reply), 'topic_id': topic_id, 'topic_assignment_reason': topic_reason,
        'views': getattr(message, 'views', None), 'forwards': getattr(message, 'forwards', None),
        'replies': clean(getattr(message, 'replies', None)),
        'reactions': clean(getattr(message, 'reactions', None)),
        'forward_origin': clean(getattr(message, 'fwd_from', None)),
        'collected_at': now(), 'video_evidence': evidence, 'exclusion_reason': reason,
        'protected': bool(source['protected'] or getattr(message, 'noforwards', False)),
        # This is the message object only, not the response's users/chats or session.
        'raw_message': clean(message),
    }
    return result


def filename(record):
    original = record['original_filename'] or 'video.mp4'
    safe = re.sub(r'[^\w.\-]+', '_', original, flags=re.UNICODE).strip('._')
    # Keep below filesystem byte limits, including multi-byte captions.
    safe = safe.encode('utf-8')[:100].decode('utf-8', errors='ignore') or 'video.mp4'
    return f'{record["source"]["id"]}_{record["message_id"]}_{record["media_id"]}_{safe}'


def cover_filename(record):
    # Same stem as the video: one cover belongs to exactly one message/media pair.
    return filename(record) + '.jpg'


def is_general(record):
    return bool(record['source'].get('forum') and record.get('topic_id') == 1)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def validate_cover(path, expected_size=None):
    """Image-only checks. Deliberately free of ffprobe/ffmpeg: covers never decode."""
    path = Path(path)
    size = path.stat().st_size
    if size == 0 or (expected_size is not None and size != expected_size):
        raise ValueError(f'cover_size_mismatch: expected={expected_size}, actual={size}')
    with path.open('rb') as f:
        if f.read(3) != JPEG_MAGIC:
            raise ValueError('cover_not_jpeg')
    return {'size': size, 'sha256': sha256(path), 'verified_at': now()}


def media_tools():
    for tool in ('ffprobe', 'ffmpeg'):
        if not shutil.which(tool):
            raise StopArchive(f'missing_tool: {tool}')


def validate_file(path, expected_size=None, expected_sha=None, decode=False):
    media_tools()
    path = Path(path)
    size = path.stat().st_size
    if size == 0 or (expected_size is not None and size != expected_size):
        raise ValueError(f'size_mismatch: expected={expected_size}, actual={size}')
    digest = sha256(path)
    if expected_sha and digest != expected_sha:
        raise ValueError('sha256_mismatch')
    probe = subprocess.run(['ffprobe', '-v', 'error', '-show_format', '-show_streams',
                            '-of', 'json', str(path)], capture_output=True, text=True, timeout=120)
    if probe.returncode:
        raise ValueError('ffprobe_failed')
    data = json.loads(probe.stdout)
    streams = [s for s in data.get('streams', []) if s.get('codec_type') == 'video'
               and not s.get('disposition', {}).get('attached_pic')]
    if not streams:
        raise ValueError('no_video_stream')
    if decode:
        # Decode the complete sampled video, not merely its container header.
        proc = subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-xerror', '-i', str(path),
                               '-map', '0:v:0', '-map', '0:a?', '-f', 'null', '-'],
                              capture_output=True, timeout=1800)
        if proc.returncode:
            raise ValueError('sample_decode_failed')
    # ffprobe's arbitrary container tags are not business metadata.
    return {'actual_size': size, 'sha256': digest, 'verified_at': now(),
            'probe': {'format': data.get('format', {}).get('format_name', ''),
                      'duration': data.get('format', {}).get('duration'),
                      'video_streams': [{k: s.get(k) for k in ('codec_name', 'width', 'height')}
                                        for s in streams]},
            'decoded_at': now() if decode else None}


class Store:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / 'videos').mkdir(exist_ok=True)
        (self.root / 'covers').mkdir(exist_ok=True)
        self.db = sqlite3.connect(self.root / 'state.sqlite3')
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS task (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS videos (
                message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                status TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pages (
                number INTEGER PRIMARY KEY, request_offset INTEGER, next_offset INTEGER,
                ids TEXT, collected_at TEXT);
            CREATE TABLE IF NOT EXISTS topics (id INTEGER PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS topic_inventories (
                source_id INTEGER, topic_id INTEGER, data TEXT NOT NULL,
                PRIMARY KEY(source_id,topic_id));
            CREATE TABLE IF NOT EXISTS covers (
                message_id INTEGER PRIMARY KEY REFERENCES messages(id),
                status TEXT NOT NULL, data TEXT NOT NULL);
        ''')

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM task WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO task VALUES (?,?)',
                        (key, json.dumps(value, ensure_ascii=False)))

    def start(self, source, started_at, upper):
        if self.get('source'):
            if self.get('source')['id'] != source['id']:
                raise StopArchive('source_changed: this directory belongs to another target')
            return
        with self.db:
            for k, v in {'source': source, 'started_at': started_at, 'upper_id': upper,
                         'cursor': upper + 1, 'scan_complete': False, 'limitations': [],
                         'sample_ids': [], 'sample_passed': False}.items():
                self.set(k, v)

    def ingest(self, records, requested_offset, next_offset, ids, topics=()):
        # Page records AND cursor commit together. Download states are independent.
        with self.db:
            for topic in topics:
                self.db.execute('INSERT OR REPLACE INTO topics VALUES (?,?)',
                                (topic.id, json.dumps(clean(topic), ensure_ascii=False)))
            self._ingest_records(records)
            self.db.execute('INSERT INTO pages(request_offset,next_offset,ids,collected_at) VALUES (?,?,?,?)',
                            (requested_offset, next_offset, json.dumps(ids), now()))
            self.set('cursor', next_offset)
            if not ids:
                self.set('scan_complete', True)

    def _ingest_records(self, records):
        for r in records:
            self.db.execute('INSERT OR IGNORE INTO messages VALUES (?,?)',
                            (r['message_id'], json.dumps(r, ensure_ascii=False)))
            if r['video_evidence']:
                status = 'skipped' if is_general(r) else 'pending'
                state = {'status': status, 'attempts': 0, 'local_path': '', 'actual_size': None,
                         'sha256': '', 'downloaded_at': None, 'verified_at': None,
                         'failure_reason': 'general_topic_excluded' if is_general(r) else '',
                         'events': []}
                self.db.execute('INSERT OR IGNORE INTO videos VALUES (?,?,?)',
                                (r['message_id'], status, json.dumps(state)))
                cover = {**COVER_STATE, 'status': status, 'events': [],
                         'failure_reason': 'general_topic_excluded' if is_general(r) else ''}
                self.db.execute('INSERT OR IGNORE INTO covers VALUES (?,?,?)',
                                (r['message_id'], status, json.dumps(cover)))
            raw = r['raw_message']
            action = (raw.get('action') or {}).get('_', '')
            media = (raw.get('media') or {}).get('_', '')
            if 'Migrate' in action or 'PaidMedia' in media:
                limitations = self.get('limitations', [])
                note = f'message {r["message_id"]}: {action or media}; not followed or purchased'
                if note not in limitations:
                    self.set('limitations', limitations + [note])

    def ingest_topic_records(self, records):
        """Add source-topic metadata without advancing global history evidence."""
        with self.db:
            for record in records:
                old = self.db.execute('SELECT data FROM messages WHERE id=?',
                                      (record['message_id'],)).fetchone()
                if old:
                    previous = json.loads(old[0])
                    if (previous['source']['id'] != record['source']['id']
                            or previous.get('media_id') != record.get('media_id')
                            or (previous.get('expected_size') is not None
                                and record.get('expected_size') is not None
                                and previous['expected_size'] != record['expected_size'])):
                        raise StopArchive('source_video_identity_changed')
            self._ingest_records(records)

    def save_topic_inventory(self, proof):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO topic_inventories VALUES (?,?,?)',
                            (proof['source_id'], proof['topic_id'],
                             json.dumps(proof, ensure_ascii=False)))

    def videos(self):
        return [(json.loads(row['message']), json.loads(row['data'])) for row in self.db.execute(
            'SELECT m.data AS message,v.data FROM videos v JOIN messages m ON m.id=v.message_id '
            'ORDER BY m.id DESC')]

    def update(self, mid, state):
        with self.db:
            self.db.execute('UPDATE videos SET status=?,data=? WHERE message_id=?',
                            (state['status'], json.dumps(state, ensure_ascii=False), mid))

    def counts(self):
        return dict(self.db.execute('SELECT status,count(*) FROM videos GROUP BY status').fetchall())

    def cover(self, mid):
        """Cover state for a message, defaulted for archives predating covers."""
        row = self.db.execute('SELECT data FROM covers WHERE message_id=?', (mid,)).fetchone()
        return json.loads(row[0]) if row else {**COVER_STATE, 'events': []}

    def update_cover(self, mid, state):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO covers VALUES (?,?,?)',
                            (mid, state['status'], json.dumps(state, ensure_ascii=False)))

    def cover_counts(self):
        return dict(self.db.execute('SELECT status,count(*) FROM covers GROUP BY status').fetchall())

    def prepare_downloads(self):
        """Apply current scope and release legacy local protection-only blocks."""
        changed = set()
        for record, state in self.videos():
            if is_general(record) and state['status'] != 'skipped':
                state.update(status='skipped', failure_reason='general_topic_excluded')
            elif state['status'] == 'blocked' and state.get('failure_reason') in (
                    'content_protected', 'source_content_protection_enabled',
                    'protected_or_expiring_media'):
                # Legacy combined reasons are rechecked for TTL before downloading.
                state.update(status='pending', failure_reason='')
            else:
                continue
            state['events'].append({'at': now(), 'event': 'download_policy_updated',
                                    'reason': state['failure_reason']})
            self.update(record['message_id'], state)
            changed.add(record['message_id'])
        if changed.intersection(self.get('sample_ids', [])):
            with self.db:
                self.set('sample_ids', [])
                self.set('sample_passed', False)

    def remaining_bytes(self):
        rows = self.videos()
        return sum(r['expected_size'] or 0 for r, s in rows if s['status'] not in ('success', 'blocked', 'skipped'))

    def export(self):
        fields = ['source_id', 'message_id', 'message_link', 'published_at', 'edited_at', 'text',
                  'title', 'description', 'episode', 'tags', 'original_filename', 'media_id',
                  'mime_type', 'duration_seconds', 'width', 'height', 'expected_size', 'album_id',
                  'topic_id', 'video_evidence', 'exclusion_reason', 'status', 'local_path',
                  'actual_size', 'sha256', 'downloaded_at', 'failure_reason',
                  'cover_status', 'cover_path', 'cover_reason']
        jtmp, ctmp = self.root / 'messages.jsonl.tmp', self.root / 'index.csv.tmp'
        with jtmp.open('w', encoding='utf-8') as jf, ctmp.open('w', encoding='utf-8-sig', newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=fields)
            writer.writeheader()
            for row in self.db.execute('SELECT m.data AS message,v.data AS state,c.data AS cover '
                                       'FROM messages m LEFT JOIN videos v ON m.id=v.message_id '
                                       'LEFT JOIN covers c ON m.id=c.message_id ORDER BY m.id'):
                record = json.loads(row['message'])
                state = json.loads(row['state']) if row['state'] else None
                # A video archived before covers existed has no row yet: that is pending,
                # not unknown, so the table can always answer "which videos lack a cover".
                cover = json.loads(row['cover']) if row['cover'] else (
                    {**COVER_STATE} if state else None)
                record['download'] = state
                record['cover'] = cover
                jf.write(json.dumps(record, ensure_ascii=False) + '\n')
                # Cover keys are mapped, never merged: both states use 'status'/'sha256'.
                flat = {**record, **(state or {}), 'source_id': record['source']['id'],
                        'cover_status': (cover or {}).get('status'),
                        'cover_path': (cover or {}).get('local_path'),
                        'cover_reason': (cover or {}).get('failure_reason')}
                out = {}
                for k in fields:
                    val = flat.get(k)
                    val = json.dumps(val, ensure_ascii=False) if isinstance(val, (list, dict)) and val else val
                    val = '' if val is None or val == [] else val
                    # CSV display export prevents spreadsheet formula execution.
                    if isinstance(val, str) and val.startswith(('=', '+', '-', '@', '\t', '\r')):
                        val = "'" + val
                    out[k] = val
                writer.writerow(out)
        os.replace(jtmp, self.root / 'messages.jsonl')
        os.replace(ctmp, self.root / 'index.csv')
        progress = {row[0]: json.loads(row[1]) for row in self.db.execute('SELECT * FROM task')}
        atomic_json(self.root / 'progress.json', progress)
        with (self.root / 'pages.jsonl.tmp').open('w', encoding='utf-8') as f:
            for row in self.db.execute('SELECT * FROM pages ORDER BY number'):
                data = dict(row)
                data['ids'] = json.loads(data['ids'])
                f.write(json.dumps(data) + '\n')
        os.replace(self.root / 'pages.jsonl.tmp', self.root / 'pages.jsonl')
        atomic_json(self.root / 'topics.json', [json.loads(r[0]) for r in self.db.execute(
            'SELECT data FROM topics ORDER BY id')])

    def pagination_audit(self):
        cursor = self.get('upper_id', 0) + 1
        seen = set()
        errors = []
        for row in self.db.execute('SELECT * FROM pages ORDER BY number'):
            ids = json.loads(row['ids'])
            if row['request_offset'] != cursor or any(i >= cursor for i in ids):
                errors.append(f'offset_mismatch_page_{row["number"]}')
            if ids != sorted(set(ids), reverse=True) or seen.intersection(ids):
                errors.append(f'duplicate_or_order_page_{row["number"]}')
            if row['next_offset'] != (min(ids) if ids else cursor):
                errors.append(f'next_offset_mismatch_page_{row["number"]}')
            seen.update(ids)
            cursor = row['next_offset']
        stored = {r[0] for r in self.db.execute('SELECT id FROM messages')}
        if stored != seen:
            errors.append('page_message_set_mismatch')
        return {'ok': not errors, 'errors': errors, 'unique_ids': len(seen),
                'pages': self.db.execute('SELECT count(*) FROM pages').fetchone()[0]}

    def report(self, operation=None):
        counts = Counter(self.counts())
        covers = Counter(self.cover_counts())
        uncovered = self.db.execute(
            'SELECT count(*) FROM videos v LEFT JOIN covers c ON c.message_id=v.message_id '
            "WHERE c.message_id IS NULL OR c.status<>'success'").fetchone()[0]
        messages = self.db.execute('SELECT count(*) FROM messages').fetchone()[0]
        rows = self.videos()
        audit = self.pagination_audit()
        times = [json.loads(r[0])['published_at'] for r in self.db.execute('SELECT data FROM messages')]
        times = [t for t in times if t]
        verified = self.get('last_verification', {})
        success = bool(self.get('source') and self.get('scan_complete') and audit['ok']
                       and not any(counts[s] for s in ('failed', 'pending', 'downloading', 'blocked'))
                       and not self.get('limitations', []) and not self.get('stop_reason')
                       and verified.get('ok') and verified.get('success_count') == counts['success']
                       and (not any(s['status'] != 'skipped' for _, s in rows) or self.get('sample_passed')))
        excluded = Counter()
        for (raw,) in self.db.execute('SELECT data FROM messages'):
            r = json.loads(raw)
            if not r['video_evidence']:
                excluded[r['exclusion_reason']] += 1
        report = {
            'generated_at': now(), 'result': 'COMPLETE_VERIFIED' if success else 'INCOMPLETE',
            'source': self.get('source') or self.get('preflight', {}).get('source'),
            'preflight': self.get('preflight'), 'started_at': self.get('started_at'),
            'upper_message_id_inclusive': self.get('upper_id'),
            'cutoff_exclusive_utc': self.get('cutoff_exclusive_utc'),
            'observed_time_range': {'earliest': min(times) if times else None, 'latest': max(times) if times else None},
            'reached_api_history_end': self.get('scan_complete', False),
            'scanned_messages': messages, 'identified_videos': len(rows),
            'video_states_mutually_exclusive': {s: counts[s] for s in ('success', 'failed', 'pending', 'downloading', 'blocked', 'skipped')},
            'excluded_nonvideo_messages': sum(excluded.values()), 'exclusion_reasons': dict(excluded),
            # Reported only: a missing cover never blocks the video completion gate.
            'cover_states': {s: covers[s] for s in ('success', 'failed', 'pending', 'downloading',
                                                    'unavailable', 'skipped')},
            'videos_without_cover': uncovered,
            'reconciliation_ok': messages == len(rows) + sum(excluded.values()) and len(rows) == sum(counts.values()),
            'expected_video_bytes': sum(r['expected_size'] or 0 for r, s in rows if s['status'] != 'skipped'),
            'unknown_size_videos': sum(r['expected_size'] is None for r, _ in rows),
            'verified_bytes': sum(s.get('actual_size') or 0 for _, s in rows if s['status'] == 'success'),
            'known_remaining_bytes': self.remaining_bytes(), 'free_disk_bytes': shutil.disk_usage(self.root).free,
            'disk_reserve_bytes': RESERVE, 'pagination_audit': audit,
            'sample_ids': self.get('sample_ids', []), 'sample_passed': self.get('sample_passed', False),
            'last_verification': verified, 'last_operation': operation or self.get('last_operation', {}),
            'stop_reason': self.get('stop_reason', ''), 'limitations': self.get('limitations', []),
            'scope_limits': ['Only accessible history within the frozen time/ID boundary.',
                             'Deleted or inaccessible messages cannot be enumerated; ID gaps are not evidence of loss.',
                             'No atomic server snapshot: edits/deletions during collection may be observed.',
                             'General (forum topic 1) is excluded from downloads; protection flags are metadata only.',
                             'Standard Telegram API only; no automatic joins, purchases, or cross-source traversal.',
                             'File-level restart only; alternate encodings are metadata, not separate videos.',
                             'Documents without video attributes, video MIME or recognized extension cannot be reliably classified as video.',
                             'All successes probed; only sampled videos fully decoded, not human visually reviewed.'],
        }
        atomic_json(self.root / 'report.json', report)
        return report
