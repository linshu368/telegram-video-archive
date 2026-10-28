"""Local-only journal and interprocess cache locks, independent of legacy SQLite."""
import fcntl
import json
import re
import shutil
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from video_archive.core import sha256
from video_archive.pipeline.common import PipelineError, identity, object_path


class Cache:
    def __init__(self, root, high=20 * 1024**3, low=12 * 1024**3, floor=5 * 1024**3,
                 project_ref=None):
        self.root = Path(root).resolve()
        self.high, self.low, self.floor = high, low, floor
        if not 0 <= low < high or floor < 0:
            raise PipelineError('invalid_cache_limits')
        self.directory = self.root / 'pipeline'
        if project_ref is not None:
            if not re.fullmatch(r'[a-z0-9]{20}', project_ref):
                raise PipelineError('invalid_cache_project')
            self.directory = self.directory / 'projects' / project_ref
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        (self.directory / 'locks').mkdir(mode=0o700, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS entries (video_id TEXT PRIMARY KEY, data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reservations (video_id TEXT PRIMARY KEY, bytes INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.directory / 'cache.sqlite3', timeout=30)
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def lock(self, vid, shared=False, blocking=True):
        # UUID validation prevents path traversal through a caller-supplied key.
        import uuid
        path = self.directory / 'locks' / str(uuid.UUID(str(vid)))
        with path.open('a') as handle:
            op = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            try:
                fcntl.flock(handle, op | (0 if blocking else fcntl.LOCK_NB))
            except BlockingIOError:
                raise PipelineError('cache_in_use') from None
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def get(self, vid):
        with self.connect() as db:
            row = db.execute('SELECT data FROM entries WHERE video_id=?', (str(vid),)).fetchone()
        return json.loads(row[0]) if row else {}

    def put(self, vid, **values):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT data FROM entries WHERE video_id=?', (str(vid),)).fetchone()
            data = json.loads(row[0]) if row else {}
            data.update(values)
            db.execute('INSERT OR REPLACE INTO entries VALUES (?,?)', (str(vid), json.dumps(data)))

    def path(self, entry):
        path = Path(entry['path'])
        if not path.is_absolute() or path.is_symlink() or not path.resolve().is_relative_to(self.root):
            raise PipelineError('unsafe_cache_path')
        return path

    def usage(self):
        # Includes partial downloads, managed AI scratch and legacy originals.
        return sum(p.stat().st_size for folder in (self.root / 'videos', self.directory / 'scratch')
                   if folder.exists() for p in folder.rglob('*') if p.is_file())

    @contextmanager
    def reserve(self, vid, needed):
        """Caller must hold exclusive video lock throughout allocation/write/release."""
        if not isinstance(needed, int) or needed <= 0:
            raise PipelineError('unknown_cache_allocation')
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # Reclaim only reservations whose OS lock is gone (crash), never by TTL.
            for other, _ in db.execute('SELECT video_id,bytes FROM reservations').fetchall():
                if other == str(vid):
                    db.execute('DELETE FROM reservations WHERE video_id=?', (other,))
                    continue
                try:
                    with self.lock(other, blocking=False):
                        db.execute('DELETE FROM reservations WHERE video_id=?', (other,))
                except PipelineError:
                    pass
            held = db.execute('SELECT coalesce(sum(bytes),0) FROM reservations').fetchone()[0]
            used = self.usage()
            paused = db.execute("SELECT value FROM settings WHERE key='paused'").fetchone()
            paused = bool(paused and paused[0])
            if paused and used + held <= self.low:
                paused = False
            denied = paused or used + held + needed > self.high or shutil.disk_usage(self.root).free - held - needed < self.floor
            db.execute("INSERT OR REPLACE INTO settings VALUES ('paused',?)", (int(denied),))
            if not denied:
                db.execute('INSERT INTO reservations VALUES (?,?)', (str(vid), needed))
        if denied:
            raise PipelineError('cache_paused')
        try:
            yield
        finally:
            with self.connect() as db:
                db.execute('DELETE FROM reservations WHERE video_id=?', (str(vid),))

    def cleanup(self, vid, database):
        with self.lock(vid, blocking=False):
            entry, state = self.get(vid), database.state(vid)
            if entry.get('protected'):
                raise PipelineError('protected_sample')
            if not state or state.get('object_path') != object_path(vid) or state['tasks']['upload']['status'] != 'succeeded' or 'ai' not in state['tasks']:
                raise PipelineError('archive_not_committed')
            if not entry.get('remote_sha256') or entry.get('remote_sha256') != entry.get('sha256'):
                raise PipelineError('remote_not_verified')
            path = self.path(entry)
            if path.exists() and sha256(path) != entry['sha256']:
                raise PipelineError('cache_changed')
            # Commit tombstone before unlink; a crash can leave an extra safe file.
            self.put(vid, archived=True, cleaned=True, object_path=object_path(vid))
            path.unlink(missing_ok=True)


def archived_local(root, record):
    """Read-only guard for legacy recovery: never re-download cloud tombstones."""
    path = Path(root) / 'pipeline/cache.sqlite3'
    if not path.exists():
        return False
    vid = identity('message', record['source']['id'], record['message_id'])
    db = sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True)
    try:
        row = db.execute('SELECT data FROM entries WHERE video_id=?', (vid,)).fetchone()
        e = json.loads(row[0]) if row else {}
        return bool(e.get('archived') and e.get('object_path') == object_path(vid)
                    and e.get('remote_sha256') and e.get('remote_sha256') == e.get('sha256'))
    finally:
        db.close()


def legacy_entry(root, vid):
    """Read-only lookup in the pre-project-scoping cache.

    Only used to recover a local sha256/size already verified against the
    remote object before per-project cache directories existed; never used
    to decide archived/not-archived (the DB's object_path/upload status is
    the only authority for that).
    """
    path = Path(root) / 'pipeline/cache.sqlite3'
    if not path.exists():
        return {}
    db = sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True)
    try:
        row = db.execute('SELECT data FROM entries WHERE video_id=?', (str(vid),)).fetchone()
        entry = json.loads(row[0]) if row else {}
        return entry if entry.get('remote_sha256') and entry.get('remote_sha256') == entry.get('sha256') else {}
    finally:
        db.close()
