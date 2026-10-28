"""Worker uses only restricted SQL functions; no Data API exposure required."""
import asyncio
import threading
import time

import psycopg
from psycopg.types.json import Jsonb

from video_archive.pipeline.common import PipelineError, error_code


class Database:
    # Pooled Supavisor sessions refuse bursts of new connections; bound them per process.
    slots = threading.BoundedSemaphore(4)
    heartbeat_slots = threading.BoundedSemaphore(2)

    def __init__(self, dsn):
        self.dsn = dsn

    def verify_worker(self):
        """Read-only runtime gate. RLS alone is not a permissions audit."""
        with self.connect() as db:
            db.execute("SET statement_timeout = '30s'")
            db.execute("SET TRANSACTION READ ONLY")
            role = db.execute("""
                SELECT session_user=current_user, rolcanlogin,
                       rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR rolreplication
                FROM pg_roles WHERE rolname=session_user
            """).fetchone()
            if not role or not role[0] or not role[1] or role[2]:
                raise PipelineError('restricted_worker_login_required')
            memberships = {r[0] for r in db.execute("""
                SELECT rolname FROM pg_roles WHERE rolname<>session_user
                  AND pg_has_role(session_user,oid,'MEMBER')
            """)}
            if memberships != {'drama_worker'}:
                raise PipelineError('worker_membership_conflict')
            group = db.execute("""
                SELECT rolcanlogin OR rolsuper OR rolbypassrls OR rolcreaterole
                       OR rolcreatedb OR rolreplication FROM pg_roles WHERE rolname='drama_worker'
            """).fetchone()
            if not group or group[0]:
                raise PipelineError('worker_role_conflict')
            if db.execute("""
                SELECT has_schema_privilege(current_user,'drama_archive','CREATE')
                    OR NOT has_schema_privilege(current_user,'drama_archive','USAGE')
            """).fetchone()[0]:
                raise PipelineError('worker_schema_privileges_conflict')
            tables = db.execute("""
                SELECT c.oid,c.relname,c.relrowsecurity,c.relowner,
                       has_table_privilege(current_user,c.oid,
                         'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
                       OR has_any_column_privilege(current_user,c.oid,'SELECT,INSERT,UPDATE,REFERENCES')
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='drama_archive' AND c.relkind IN ('r','p','v','m','f')
            """).fetchall()
            if {r[1] for r in tables} != {'dramas', 'videos', 'ai_results', 'pipeline_tasks', 'drama_tasks'}:
                raise PipelineError('business_schema_conflict')
            if any(not r[2] or r[4] or db.execute(
                    "SELECT pg_has_role(current_user,%s,'MEMBER')", (r[3],)).fetchone()[0] for r in tables):
                raise PipelineError('worker_table_privileges_conflict')
            funcs = db.execute("""
                SELECT p.oid,p.proname,p.prosecdef,p.proconfig,p.proowner,
                       has_function_privilege(current_user,p.oid,'EXECUTE'),
                       EXISTS (SELECT FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner)))
                               WHERE grantee=0 AND privilege_type='EXECUTE')
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='drama_archive'
            """).fetchall()
            if len(funcs) != 10 or {r[1] for r in funcs} != {
                    'import_video', 'video_state', 'archived_messages', 'claim', 'renew', 'finish', 'fail',
                    'regenerate_ai', 'regenerate_transcode', 'ai_source_video'}:
                raise PipelineError('worker_function_contract_conflict')
            for _, _, definer, settings, owner, execute, public in funcs:
                if (not definer or settings != ['search_path=pg_catalog, drama_archive']
                        or not execute or public or db.execute(
                            "SELECT pg_has_role(current_user,%s,'MEMBER')", (owner,)).fetchone()[0]):
                    raise PipelineError('worker_function_privileges_conflict')
            return {'restricted_login': True, 'five_tables_rls': True,
                    'direct_table_access': False}

    def connect(self):
        """Retry only the connection attempt: nothing has been sent, so it is idempotent."""
        for attempt in range(4):
            try:
                return psycopg.connect(self.dsn, connect_timeout=15)
            except psycopg.OperationalError:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)

    def call(self, name, *args):
        allowed = {'import_video', 'video_state', 'archived_messages', 'claim', 'renew', 'finish', 'fail',
                   'regenerate_ai', 'regenerate_transcode', 'ai_source_video'}
        if name not in allowed:
            raise PipelineError('invalid_database_operation')
        slots = self.heartbeat_slots if name == 'renew' else self.slots
        with slots, self.connect() as db:
            db.execute("SET statement_timeout = '30s'")
            return db.execute(f'SELECT drama_archive.{name}({",".join(["%s"] * len(args))})',
                              [Jsonb(x) if isinstance(x, dict) else x for x in args]).fetchone()[0]

    def archived_messages(self):
        """Message ids whose upload is committed, via restricted functions only."""
        return {int(m) for m in self.call('archived_messages')}

    def state(self, vid):
        return self.call('video_state', vid)

    def claim(self, vid, stage, seconds=120, attempts=5, backoff=30):
        return self.call('claim', vid, stage, seconds, attempts, backoff)


class Lease:
    """Dedicated connections for heartbeat; fail closed on ambiguous renewal."""
    def __init__(self, db, vid, stage, seconds=120):
        self.db, self.vid, self.stage, self.seconds = db, vid, stage, seconds
        self.token = None
        self.stop = threading.Event()
        self.lost = threading.Event()

    def __enter__(self):
        self.token = self.db.claim(self.vid, self.stage, self.seconds)
        if not self.token:
            raise PipelineError('task_not_claimable')
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()
        return self

    async def __aenter__(self):
        pending = asyncio.create_task(asyncio.to_thread(self.__enter__))
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            # An in-flight claim can succeed after cancellation. Join its heartbeat
            # before the process lock is released; expiry handles the task itself.
            try:
                await asyncio.shield(pending)
            finally:
                if hasattr(self, 'thread'):
                    await asyncio.to_thread(self.__exit__, None, None, None)
            raise

    async def __aexit__(self, kind, exc, tb):
        await asyncio.to_thread(self.__exit__, kind, exc, tb)

    def _heartbeat(self):
        while not self.stop.wait(self.seconds / 3):
            try:
                if not self.db.call('renew', self.vid, self.stage, self.token, self.seconds):
                    self.lost.set()
                    return
            except Exception:
                self.lost.set()
                return

    def check(self):
        if self.lost.is_set():
            raise PipelineError('lease_lost')

    def fence(self):
        self.check()
        if not self.db.call('renew', self.vid, self.stage, self.token, self.seconds):
            self.lost.set()
            raise PipelineError('lease_lost')

    def finish(self, payload=None):
        self.check()
        self.db.call('finish', self.vid, self.stage, self.token, payload or {})

    def __exit__(self, kind, exc, tb):
        self.stop.set()
        self.thread.join()
        if exc is not None:
            try:
                self.db.call('fail', self.vid, self.stage, self.token, error_code(exc))
            except Exception:
                pass  # Ambiguous commit: next invocation reconciles DB + object.
