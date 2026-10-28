"""Offline test doubles + optional real PostgreSQL transaction/permission tests."""
import asyncio
import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import archive
from core import Store, filename, record_message
from pipeline import archive_existing, download_task, main, run_ai, sample_evidence
from pipeline_cache import Cache, archived_local
from pipeline_common import (BASE, PipelineError, ai_output, config, cover_object_path,
                             covers_committed, display, error_code, identity, object_path,
                             snapshot)
from pipeline_db import Database, Lease
from pipeline_storage import CHUNK, COVER_LIMIT, COVERS, Storage
from test_archive import FakeClient, SOURCE, message

PROJECT = 'a' * 20


def fixture(mid=6558, source=-100123, topic=20):
    did, vid = identity('topic', source, topic), identity('message', source, mid)
    return ({'id': did, 'title': '剧名', 'source_id': str(source), 'topic_id': str(topic)},
            {'id': vid, 'drama_id': did, 'source_id': str(source), 'message_id': str(mid),
             'media_id': str(9000 + mid), 'original_title': None, 'original_tags': [],
             'original_description': None, 'episode': None})


class CommonTests(unittest.TestCase):
    def test_cli_rejects_owner_before_storage_or_import(self):
        rows = [(d, v, {'message_id': mid, 'expected_size': 10}, {})
                for mid in (6558, 6559) for d, v in [fixture(mid)]]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch('pipeline.BASE', root), patch('pipeline.snapshot', return_value=(rows, {})), \
                    patch('pipeline.config', return_value={'database_dsn': 'test', 'global_file_limit': 100,
                                                          'bucket_file_limit': 100, 'project_ref': 'a'*20}), \
                    patch('pipeline.Database') as database, patch('pipeline.Storage') as storage, \
                    patch('pipeline.import_rows') as importer, \
                    patch.object(sys, 'argv', ['pipeline.py', '--data', str(root), 'samples']):
                database.return_value.verify_worker.side_effect = PipelineError('restricted_worker_login_required')
                self.assertEqual(main(), 2)
                storage.return_value.bucket.assert_not_called()
                importer.assert_not_called()

    def test_incremental_migration_only_accepts_an_explicit_known_file(self):
        rows = [(d, v, {'message_id': mid, 'expected_size': 10}, {})
                for mid in (6558, 6559) for d, v in [fixture(mid)]]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'migrations').mkdir()
            with patch('pipeline.BASE', root), patch('pipeline.snapshot', return_value=(rows, {})), \
                    patch('pipeline.config', return_value={'database_dsn': 'test', 'global_file_limit': 100,
                                                          'bucket_file_limit': 100, 'project_ref': 'a'*20}), \
                    patch('pipeline.Database'), patch('pipeline.Storage'), \
                    patch('psycopg.connect') as connect:
                for name in ('../004_cover.sql', '004_cover.sql; DROP SCHEMA x',
                             '001_pipeline.sql', '004_cover.sql'):
                    with patch.object(sys, 'argv', ['pipeline.py', '--data', str(root),
                                                    'migrate', '--migration', name]):
                        self.assertEqual(main(), 2)
                # Rejected before any database connection is opened.
                connect.assert_not_called()

    def test_partial_sample_report_retains_successful_evidence(self):
        rows = [(d, v, {'message_id': mid, 'expected_size': 10}, {})
                for mid in (6558, 6559) for d, v in [fixture(mid)]]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            original = root / 'sample'; original.write_bytes(b'original')
            with patch('pipeline.BASE', root), patch('pipeline.snapshot', return_value=(rows, {})), \
                    patch('pipeline.config', return_value={'database_dsn': 'test', 'global_file_limit': 100,
                                                          'bucket_file_limit': 100, 'project_ref': 'a'*20}), \
                    patch('pipeline.Database') as database, patch('pipeline.Storage'), \
                    patch('pipeline.import_rows'), patch('pipeline.backup_legacy'), \
                    patch('pipeline.archive_existing', side_effect=['uploaded_readback_verified', OSError('private detail')]), \
                    patch('pipeline.sample_evidence', return_value={'message_id': 6558, 'remote_sha256': 'test-only'}), \
                    patch('pipeline.local_file', return_value=original), \
                    patch.object(sys, 'argv', ['pipeline.py', '--data', str(root), 'samples']):
                database.return_value.verify_worker.return_value = {'test_double': True}
                self.assertEqual(main(), 2)
            report = json.loads((root / 'pipeline-remote-acceptance.json').read_text())
            self.assertEqual(report['status'], 'incomplete')
            self.assertEqual(report['error'], 'builtins_OSError')
            self.assertEqual([s['message_id'] for s in report['samples']], [6558])
            self.assertEqual(len(list(root.glob('pipeline-remote-20*.json'))), 1)
            self.assertTrue(original.exists())

    def test_config_init_never_overwrites(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'pipeline.json'
            args = ['pipeline.py', '--config', str(path), 'init-config']
            with patch.object(sys, 'argv', args):
                self.assertEqual(main(), 0)
                before = path.read_bytes()
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                self.assertEqual(main(), 2)
                self.assertEqual(path.read_bytes(), before)

    def test_config_requires_evidence_and_rejects_target_overrides(self):
        from datetime import datetime, timezone
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'pipeline.json'
            c = dict(project_ref='a'*20, region='test-region', plan='pro',
                     database_dsn='host=db.'+'a'*20+'.supabase.co sslmode=verify-full',
                     storage_key='test-only', global_file_limit=100000000,
                     bucket_file_limit=100000000, sample_budget_confirmed=True,
                     dashboard_verified_at=datetime.now(timezone.utc).isoformat(),
                     sample_budget_note='test fixture only')
            path.write_text(json.dumps(c)); path.chmod(0o600)
            self.assertEqual(config(path)['project_ref'], 'a'*20)
            for override in (' hostaddr=127.0.0.1', ' service=elsewhere', " options='-c role=postgres'"):
                path.write_text(json.dumps({**c, 'database_dsn': c['database_dsn']+override}))
                with self.assertRaisesRegex(PipelineError, 'override_forbidden'):
                    config(path)
            pooler = 'host=aws-1-ap-southeast-1.pooler.supabase.com port=5432 user=%s dbname=postgres sslmode=require'
            path.write_text(json.dumps({**c, 'database_dsn': pooler % ('postgres.' + 'a'*20)}))
            self.assertEqual(config(path)['project_ref'], 'a'*20)
            for bad in (pooler % ('postgres.' + 'b'*20), pooler.replace('5432', '6543') % ('postgres.' + 'a'*20),
                        (pooler % ('postgres.' + 'a'*20)).replace('aws-1-ap-southeast-1.pooler', 'evil.example')):
                path.write_text(json.dumps({**c, 'database_dsn': bad}))
                with self.assertRaisesRegex(PipelineError, 'database_project_mismatch'):
                    config(path)
            for key, value in [('dashboard_verified_at', ''),('dashboard_verified_at', '2020-01-01T00:00:00+00:00'),
                               ('sample_budget_note', ' '), ('bucket_file_limit', True)]:
                path.write_text(json.dumps({**c, key: value}))
                with self.assertRaises(PipelineError):
                    config(path)

    def test_identity_stable_and_separated(self):
        self.assertEqual(identity('message', 1, 2), identity('message', '1', '2'))
        self.assertNotEqual(identity('message', 1, 2), identity('message', 2, 2))
        self.assertNotEqual(identity('message', 1, 2), identity('topic', 1, 2))

    def test_field_fallback_whitespace_no_merge(self):
        v = {'original_title': '  ', 'original_tags': [' 原 ', '', '原'], 'original_description': '简介'}
        a = {'title': 'AI', 'tags': ['AI'], 'description': 'AI简介'}
        self.assertEqual(display(v, a), {'title': 'AI', 'tags': ['原'], 'description': '简介', 'episode': None})
        self.assertEqual(v['original_title'], '  ')
        self.assertEqual(display({'original_tags': [' ']}, a)['tags'], ['AI'])
        self.assertEqual(display({})['tags'], [])

    def test_ai_strict_validation(self):
        for invalid in ({}, {'title': 1, 'tags': [], 'description': None},
                        {'title': None, 'tags': 'bad', 'description': None},
                        {'title': None, 'tags': [None], 'description': None}):
            with self.assertRaises(PipelineError):
                ai_output(invalid)
        self.assertEqual(ai_output({'title': ' ', 'tags': [' a ', 'a'], 'description': None}),
                         {'title': None, 'tags': ['a'], 'description': None})

    def test_errors_do_not_leak(self):
        for exc in [ValueError('secret https://signed?token=key'), PipelineError('secret token')]:
            self.assertNotIn('secret', error_code(exc))

    def test_config_permissions_and_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'pipeline.json'
            with self.assertRaisesRegex(PipelineError, 'missing'):
                config(p)
            p.write_text('{}')
            p.chmod(0o644)
            with self.assertRaisesRegex(PipelineError, 'permissions'):
                config(p)
            p.chmod(0o600)
            with self.assertRaisesRegex(PipelineError, 'incomplete'):
                config(p)

    def test_select_ai_candidates_order_and_limit(self):
        from pipeline_batch import select_ai_candidates
        rows = [(None, None, {'message_id': i}, None) for i in (5, 3, 9, 7, 2)]
        self.assertEqual([r[2]['message_id'] for r in select_ai_candidates(rows, {3, 7, 9})], [3, 7, 9])
        self.assertEqual([r[2]['message_id'] for r in select_ai_candidates(rows, {3, 7, 9}, limit=2)], [3, 7])
        # Cloud-committed ids absent from the local inventory are not an error.
        self.assertEqual(select_ai_candidates(rows, {999}), [])
        for bad in (0, -1, True):
            with self.assertRaises(PipelineError):
                select_ai_candidates(rows, {3}, limit=bad)

    def test_system_prompt_missing_and_present(self):
        from pipeline_ai import system_prompt
        with patch('pipeline_ai.BASE', Path('/nonexistent-dir-for-test-only')):
            with self.assertRaisesRegex(PipelineError, 'ai_system_prompt_missing'):
                system_prompt()
        self.assertIn('18', system_prompt())  # real prompt.md ships with the repo

    def test_strip_fence(self):
        from pipeline_ai import strip_fence
        self.assertEqual(strip_fence('```json\n{"a":1}\n```'), '{"a":1}')
        self.assertEqual(strip_fence('  {"a":1}  '), '{"a":1}')

    def test_call_model_sends_frames_with_auth_and_parses_fenced_json(self):
        from pipeline_ai import call_model
        cfg = {'model': 'm', 'api_key': 'k', 'base_url': 'https://api.example.com/v1', 'request_timeout': 5}
        response = Mock()
        response.read.return_value = json.dumps({'choices': [{'message': {
            'content': '```json\n{"title": "t", "tags": ["a"], "description": null}\n```'}}]}).encode()
        response.__enter__, response.__exit__ = Mock(return_value=response), Mock(return_value=False)
        with patch('pipeline_ai.urllib.request.urlopen', return_value=response) as urlopen:
            result = call_model([b'frame-bytes'], '', cfg, lambda: None)
        self.assertEqual(result, {'title': 't', 'tags': ['a'], 'description': None})
        request = urlopen.call_args[0][0]
        self.assertEqual(request.full_url, 'https://api.example.com/v1/chat/completions')
        self.assertEqual(request.get_header('Authorization'), 'Bearer k')
        body = json.loads(request.data)
        self.assertEqual(body['model'], 'm')
        image_parts = [p for p in body['messages'][1]['content'] if p['type'] == 'image_url']
        self.assertEqual(len(image_parts), 1)
        self.assertTrue(image_parts[0]['image_url']['url'].startswith('data:image/jpeg;base64,'))
        # No transcript: no extra text part beyond the leading instruction.
        text_parts = [p for p in body['messages'][1]['content'] if p['type'] == 'text']
        self.assertEqual(len(text_parts), 1)
        with patch('pipeline_ai.urllib.request.urlopen', return_value=response) as urlopen:
            call_model([b'frame-bytes'], '字幕转写文字', cfg, lambda: None)
        body = json.loads(urlopen.call_args[0][0].data)
        text_parts = [p for p in body['messages'][1]['content'] if p['type'] == 'text']
        self.assertEqual(len(text_parts), 2)
        self.assertIn('字幕转写文字', text_parts[1]['text'])

    def test_ai_config_permissions_and_fields(self):
        from pipeline_common import ai_config
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'ai.json'
            with self.assertRaisesRegex(PipelineError, 'missing'):
                ai_config(p)
            p.write_text('{}')
            p.chmod(0o644)
            with self.assertRaisesRegex(PipelineError, 'permissions'):
                ai_config(p)
            p.chmod(0o600)
            with self.assertRaisesRegex(PipelineError, 'incomplete'):
                ai_config(p)
            base = {'provider': 'x', 'model': 'y', 'api_key': 'z', 'base_url': 'https://api.example.com/v1'}
            p.write_text(json.dumps({**base, 'frame_count': 99}))
            with self.assertRaisesRegex(PipelineError, 'invalid_frame_sampling'):
                ai_config(p)
            p.write_text(json.dumps({**base, 'base_url': 'http://api.example.com/v1'}))
            with self.assertRaisesRegex(PipelineError, 'invalid_ai_base_url'):
                ai_config(p)
            p.write_text(json.dumps({**base, 'base_url': base['base_url'] + '/'}))
            c = ai_config(p)
            self.assertEqual(c['base_url'], base['base_url'])
            p.write_text(json.dumps(base))
            c = ai_config(p)
            self.assertEqual(c['frame_count'], 6)
            self.assertEqual(c['max_dim'], 768)
            self.assertEqual(c['request_timeout'], 120)

    def test_import_snapshot_readonly_excludes_general_missing_topics(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            records = []
            for mid, tid in [(4, 20), (3, 1), (2, None), (1, 21)]:
                r = record_message(message(mid), {**SOURCE, 'forum': True})
                r['topic_id'] = tid
                records.append(r)
            store.ingest(records, 5, 1, [4, 3, 2, 1])
            store.db.close()
            before = (Path(tmp) / 'state.sqlite3').read_bytes()
            topics = Path(tmp) / 'topics.json'
            topics.write_text(json.dumps([{'source_id': SOURCE['id'], 'topic_id': 20, 'topic_title': '剧名'}]))
            first, excluded = snapshot(tmp, topics)
            second, _ = snapshot(tmp, topics)
            self.assertEqual(first, second)
            self.assertEqual(len(first), 1)
            self.assertEqual(excluded, {'general': 1, 'missing_topic': 2})
            self.assertEqual(before, (Path(tmp) / 'state.sqlite3').read_bytes())


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / 'videos').mkdir()
        self.cache = Cache(self.root, high=100, low=40, floor=0)
        self.vid = fixture()[1]['id']

    def tearDown(self):
        self.tmp.cleanup()

    def test_reservations_and_hysteresis(self):
        other = fixture(6559)[1]['id']
        with self.cache.lock(self.vid), self.cache.reserve(self.vid, 70):
            with self.cache.lock(other), self.assertRaisesRegex(PipelineError, 'paused'):
                with self.cache.reserve(other, 40):
                    self.fail('overcommitted')
        path = self.root / 'videos/used'
        path.write_bytes(b'x' * 50)
        with self.cache.lock(other), self.assertRaisesRegex(PipelineError, 'paused'):
            with self.cache.reserve(other, 1):
                pass
        path.unlink()
        with self.cache.lock(other), self.cache.reserve(other, 20):
            pass

    def test_concurrent_reservations_only_one_fits(self):
        barrier = threading.Barrier(2)
        release = threading.Event()
        def worker(vid):
            with self.cache.lock(vid):
                barrier.wait()
                try:
                    with self.cache.reserve(vid, 70):
                        release.wait(0.3)
                        return True
                except PipelineError:
                    release.set()
                    return False
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sorted(pool.map(worker, [self.vid, fixture(6559)[1]['id']])), [False, True])

    def test_floor_and_unknown_size(self):
        self.cache.floor = 10**20
        for size in (0, None, 1):
            with self.cache.lock(self.vid), self.assertRaises(PipelineError):
                with self.cache.reserve(self.vid, size):
                    pass

    def test_crash_reservation_reclaimed(self):
        with self.cache.connect() as db:
            db.execute('INSERT INTO reservations VALUES (?,?)', (fixture(6559)[1]['id'], 90))
        with self.cache.lock(self.vid), self.cache.reserve(self.vid, 70):
            pass

    def test_cleanup_requires_commit_hash_and_no_reader(self):
        p = self.root / 'videos/test'
        p.write_bytes(b'data')
        digest = hashlib.sha256(b'data').hexdigest()
        self.cache.put(self.vid, path=str(p), sha256=digest, remote_sha256=digest)
        db = Mock()
        db.state.return_value = {'object_path': object_path(self.vid), 'tasks': {'upload': {'status': 'failed'}, 'ai': {}}}
        with self.assertRaisesRegex(PipelineError, 'not_committed'):
            self.cache.cleanup(self.vid, db)
        self.assertTrue(p.exists())
        db.state.return_value['tasks']['upload']['status'] = 'succeeded'
        with self.cache.lock(self.vid, shared=True), self.assertRaisesRegex(PipelineError, 'in_use'):
            self.cache.cleanup(self.vid, db)
        self.cache.cleanup(self.vid, db)
        self.assertFalse(p.exists())
        self.assertTrue(self.cache.get(self.vid)['archived'])

    def test_protected_original_never_deleted(self):
        self.cache.put(self.vid, protected=True)
        with self.assertRaisesRegex(PipelineError, 'protected_sample'):
            self.cache.cleanup(self.vid, Mock())

    def test_legacy_recovery_skips_cloud_without_telegram(self):
        r = record_message(message(6558), SOURCE)
        vid = identity('message', SOURCE['id'], 6558)
        self.cache.put(vid, archived=True, object_path=object_path(vid), sha256='abc', remote_sha256='abc')
        self.assertTrue(archived_local(self.root, r))
        store = Store(self.root)
        store.ingest([r], 6559, 6558, [6558])
        client = FakeClient([])
        result = asyncio.run(archive.download_one(client, None, SOURCE, store, r, store.videos()[0][1]))
        self.assertEqual(result, 'cloud_archived_skipped')
        self.assertEqual(client.downloads, 0)
        store.db.close()


class Response(io.BytesIO):
    def __init__(self, data=b'', headers=None):
        super().__init__(data)
        self.headers = headers or {}


class TusDouble(Storage):
    """Protocol test double, NOT a real Supabase endpoint."""
    def __init__(self):
        super().__init__({'project_ref': 'a' * 20, 'storage_key': 'test-only',
                          'global_file_limit': 10 * CHUNK, 'bucket_file_limit': 10 * CHUNK})
        self.content, self.uploading = None, bytearray()
        self.posts = self.patches = 0
        self.fail_patch = False
        self.expired = False

    def request(self, method, url, body=None, headers=None):
        self.check_url(url)
        if method == 'GET':
            if self.content is None:
                raise HTTPError(url, 404, 'not found', {}, None)
            return Response(self.content)
        if method == 'POST':
            self.posts += 1
            self.uploading = bytearray()
            self.size = int(headers['Upload-Length'])
            return Response(headers={'Location': self.base + '/upload/resumable/test', 'Upload-Offset': '0'})
        if method == 'HEAD':
            if self.expired:
                self.expired = False
                raise HTTPError(url, 410, 'expired', {}, None)
            return Response(headers={'Upload-Offset': str(len(self.uploading)), 'Upload-Length': str(self.size)})
        if method == 'PATCH':
            self.patches += 1
            if self.fail_patch and self.patches == 2:
                raise OSError('simulated disconnect')
            assert len(body) <= CHUNK
            assert int(headers['Upload-Offset']) == len(self.uploading)
            self.uploading.extend(body)
            if len(self.uploading) == self.size:
                self.content = bytes(self.uploading)
            return Response(headers={'Upload-Offset': str(len(self.uploading))})
        raise AssertionError(method)


class ObjectDouble(Storage):
    """Protocol test double for plain object POST/GET, NOT a real Supabase endpoint."""
    def __init__(self):
        super().__init__({'project_ref': 'a' * 20, 'storage_key': 'test-only',
                          'global_file_limit': 10 * CHUNK, 'bucket_file_limit': 10 * CHUNK})
        self.objects, self.posts = {}, 0

    def request(self, method, url, body=None, headers=None):
        self.check_url(url)
        key = url.split('/storage/v1/object/')[-1].replace('authenticated/', '', 1)
        if method == 'GET':
            if key not in self.objects:
                raise HTTPError(url, 404, 'not found', {}, None)
            return Response(self.objects[key])
        if method == 'POST':
            self.posts += 1
            if key in self.objects:
                raise HTTPError(url, 409, 'already exists', {}, None)
            assert headers['Content-Type'] == 'image/jpeg', headers
            assert 'max-age=' in headers['Cache-Control'], headers
            assert headers['x-upsert'] == 'false', headers
            self.objects[key] = body
            return Response(b'{}')
        raise AssertionError(method)


class CoverStorageTests(unittest.TestCase):
    def setUp(self):
        self.storage = ObjectDouble()
        self.vid = identity('message', -100123, 6558)
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'cover.jpg'
        self.data = b'\xff\xd8\xff' + b'cover'
        self.path.write_bytes(self.data)
        self.digest = hashlib.sha256(self.data).hexdigest()

    def tearDown(self):
        self.tmp.cleanup()

    def test_cover_upload_is_readback_verified_and_never_posts_twice(self):
        self.assertEqual(self.storage.upload_cover(self.vid, self.path, self.digest), self.digest)
        key = f'{COVERS}/{cover_object_path(self.vid)}'
        self.assertEqual(self.storage.objects[key], self.data)
        # A crash between POST and the DB commit reconciles without a second POST.
        self.assertEqual(self.storage.upload_cover(self.vid, self.path, self.digest), self.digest)
        self.assertEqual(self.storage.posts, 1)

    def test_cover_upload_refuses_a_conflicting_or_oversized_object(self):
        self.storage.objects[f'{COVERS}/{cover_object_path(self.vid)}'] = b'\xff\xd8\xffother'
        with self.assertRaisesRegex(PipelineError, 'remote_cover_conflict'):
            self.storage.upload_cover(self.vid, self.path, self.digest)
        self.storage.cover_limit = 4
        with self.assertRaisesRegex(PipelineError, 'cover_file_limit'):
            self.storage.upload_cover(identity('message', -100123, 6559), self.path, self.digest)
        self.assertEqual(self.storage.posts, 0)

    def test_cover_upload_refuses_a_file_that_changed_under_it(self):
        self.path.write_bytes(b'\xff\xd8\xffchanged')
        with self.assertRaisesRegex(PipelineError, 'local_cover_changed'):
            self.storage.upload_cover(self.vid, self.path, self.digest)
        self.assertEqual(self.storage.posts, 0)

    def test_cover_bucket_must_be_public_and_size_capped(self):
        storage = Storage({'project_ref': 'a' * 20, 'storage_key': 'test-only',
                           'global_file_limit': 10 * CHUNK, 'bucket_file_limit': 10 * CHUNK})
        self.assertEqual(storage.cover_limit, COVER_LIMIT)
        def answer(info):
            # Each call consumes its own response body.
            return lambda *a, **k: Response(json.dumps(info).encode())
        for info in ({'public': False, 'file_size_limit': COVER_LIMIT},
                     {'public': True, 'file_size_limit': 10 * CHUNK},
                     {'public': True, 'file_size_limit': COVER_LIMIT,
                      'allowed_mime_types': ['image/jpeg']}):
            with patch.object(storage, 'request', side_effect=answer(info)):
                with self.assertRaisesRegex(PipelineError, 'bucket_configuration_conflict'):
                    storage.cover_bucket()
        with patch.object(storage, 'request',
                          side_effect=answer({'public': True, 'file_size_limit': COVER_LIMIT})):
            self.assertEqual(storage.cover_bucket(),
                             {'name': COVERS, 'private': False, 'file_size_limit': COVER_LIMIT})
            # The video bucket's own contract is unchanged by the cover bucket.
            with self.assertRaisesRegex(PipelineError, 'bucket_configuration_conflict'):
                storage.bucket()


class StorageTests(unittest.TestCase):
    def test_request_setup_retries_only_reads_with_finite_attempts(self):
        storage = Storage({'project_ref':'a'*20, 'storage_key':'test-only',
                           'global_file_limit':100, 'bucket_file_limit':100})
        response = Response(b'[]')
        storage.opener = Mock()
        storage.opener.open.side_effect = [URLError('fixture'), response]
        with patch('pipeline_storage.time.sleep'):
            self.assertIs(storage.request('GET', storage.base+'/bucket'), response)
        self.assertEqual(storage.opener.open.call_count, 2)
        for method in ('POST','PATCH','GET'):
            storage.opener.open.reset_mock()
            storage.opener.open.side_effect = URLError('fixture')
            with patch('pipeline_storage.time.sleep'), self.assertRaises(URLError):
                storage.request(method, storage.base+'/upload/resumable')
            self.assertEqual(storage.opener.open.call_count, 3 if method=='GET' else 1)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'file'
        self.path.write_bytes(b'a' * (CHUNK + 10))
        self.digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        self.cache = Cache(self.tmp.name)
        self.vid = fixture()[1]['id']
        self.storage = TusDouble()

    def tearDown(self):
        self.tmp.cleanup()

    def test_resume_restart_and_readback(self):
        self.storage.fail_patch = True
        with self.assertRaises(OSError):
            self.storage.upload(self.vid, self.path, self.digest, self.cache)
        self.assertTrue(self.path.exists())
        self.storage.fail_patch = False
        restarted_cache = Cache(self.tmp.name)
        self.assertEqual(self.storage.upload(self.vid, self.path, self.digest, restarted_cache), self.digest)
        self.assertEqual(self.storage.posts, 1)

    def test_existing_object_reconciles_without_upload(self):
        self.storage.content = self.path.read_bytes()
        self.assertEqual(self.storage.upload(self.vid, self.path, self.digest, self.cache), self.digest)
        self.assertEqual(self.storage.posts, 0)

    def test_same_size_corrupt_object_conflict(self):
        self.storage.content = b'b' * self.path.stat().st_size
        with self.assertRaisesRegex(PipelineError, 'object_conflict'):
            self.storage.upload(self.vid, self.path, self.digest, self.cache)
        self.assertEqual(self.storage.posts, 0)

    def test_expired_session_recreated(self):
        self.storage.fail_patch = True
        with self.assertRaises(OSError):
            self.storage.upload(self.vid, self.path, self.digest, self.cache)
        self.storage.expired = True
        self.storage.fail_patch = False
        self.storage.upload(self.vid, self.path, self.digest, self.cache)
        self.assertEqual(self.storage.posts, 2)

    def test_untrusted_url_and_limit_fail_closed(self):
        with self.assertRaises(PipelineError):
            self.storage.check_url('https://evil.example/steal')
        self.storage.limit = 1
        with self.assertRaisesRegex(PipelineError, 'file_limit'):
            self.storage.upload(self.vid, self.path, self.digest, self.cache)


@unittest.skipUnless(os.environ.get('DRAMA_TEST_DSN'), 'requires isolated local PostgreSQL DRAMA_TEST_DSN')
class PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.dsn = os.environ['DRAMA_TEST_DSN']
        # Explicit test DB only. Never accept a Supabase or production database.
        from psycopg.conninfo import conninfo_to_dict
        info = conninfo_to_dict(cls.dsn)
        if (info.get('host') != '127.0.0.1' or info.get('dbname') != 'drama_pipeline_test'
                or info.get('port') != '55440' or any(k in info for k in ('hostaddr', 'service', 'options'))
                or any(os.environ.get(k) for k in ('PGHOSTADDR', 'PGSERVICE', 'PGOPTIONS'))):
            raise RuntimeError('isolated_local_test_database_required')
        cls.owner = psycopg.connect(cls.dsn, autocommit=True)
        actual = cls.owner.execute("""
            SELECT current_database(),host(inet_server_addr()),inet_server_port(),
                   current_setting('data_directory')
        """).fetchone()
        if (actual[:3] != ('drama_pipeline_test', '127.0.0.1', 55440)
                or Path(actual[3]).resolve() != (BASE / '.private/pipeline-test-postgres').resolve()):
            cls.owner.close()
            raise RuntimeError('isolated_local_test_instance_required')
        for role in ('anon', 'authenticated', 'service_role'):
            if not cls.owner.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
                cls.owner.execute(psycopg.sql.SQL('CREATE ROLE {} NOLOGIN').format(psycopg.sql.Identifier(role)))
        cls.owner.execute('DROP SCHEMA IF EXISTS drama_archive CASCADE')
        for name in ('001_pipeline.sql', '002_archived_messages.sql',
                     '003_import_video_concurrency.sql', '004_cover.sql',
                     '005_transcode.sql', '006_regenerate_transcode.sql', '007_drama_ai.sql'):
            cls.owner.execute((BASE / 'migrations' / name).read_text())
        cls.db = Database(cls.dsn + " options='-c role=drama_worker'")
        cls.login = 'drama_test_' + uuid.uuid4().hex[:12]
        cls.owner.execute(psycopg.sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS').format(
            psycopg.sql.Identifier(cls.login)))
        cls.owner.execute(psycopg.sql.SQL('GRANT drama_worker TO {}').format(psycopg.sql.Identifier(cls.login)))
        from psycopg.conninfo import make_conninfo
        cls.worker = Database(make_conninfo(cls.dsn, user=cls.login))

    @classmethod
    def tearDownClass(cls):
        import psycopg
        cls.owner.execute(psycopg.sql.SQL('DROP OWNED BY {}').format(psycopg.sql.Identifier(cls.login)))
        cls.owner.execute(psycopg.sql.SQL('DROP ROLE {}').format(psycopg.sql.Identifier(cls.login)))
        cls.owner.close()

    def setUp(self):
        self.owner.execute('TRUNCATE drama_archive.pipeline_tasks,drama_archive.drama_tasks,'
                           'drama_archive.ai_results,drama_archive.videos,drama_archive.dramas')
        self.d, self.v = fixture()
        self.vid = self.v['id']
        self.db.call('import_video', self.d, self.v)

    def complete(self, stage, payload=None):
        # 'ai' is keyed by drama_id, not video_id, and needs to name which video
        # fed the call; every other stage is unchanged, per-video.
        cid = self.d['id'] if stage == 'ai' else self.vid
        payload = dict(payload or {})
        if stage == 'ai':
            payload.setdefault('source_video_id', self.vid)
        token = self.db.claim(cid, stage, backoff=0)
        self.assertIsNotNone(token)
        self.db.call('finish', cid, stage, token, payload)

    def uploaded(self):
        self.complete('download')
        self.complete('upload', {'object_path': object_path(self.vid)})

    def expire(self, stage='download'):
        if stage == 'ai':
            self.owner.execute("UPDATE drama_archive.drama_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE drama_id=%s", (self.d['id'],))
        else:
            self.owner.execute("UPDATE drama_archive.pipeline_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE video_id=%s AND stage=%s", (self.vid, stage))

    def cover_path(self):
        return cover_object_path(self.vid)

    def test_cover_stages_are_independent_of_the_video_stages(self):
        state = self.db.state(self.vid)
        self.assertIsNone(state['cover_path'])
        self.assertIsNone(state['cover_note'])
        # Claimable with no video download/upload progress at all.
        self.complete('cover_download')
        self.complete('cover_upload', {'cover_path': self.cover_path()})
        state = self.db.state(self.vid)
        self.assertEqual(state['cover_path'], self.cover_path())
        self.assertIsNone(state['object_path'])
        self.assertEqual(state['tasks']['download']['status'], 'pending')
        # And the video track still runs afterwards, untouched by the cover.
        self.uploaded()
        self.assertEqual(self.db.state(self.vid)['object_path'], object_path(self.vid))

    def test_cover_upload_requires_its_own_download_and_a_matching_path(self):
        self.assertIsNone(self.db.claim(self.vid, 'cover_upload', backoff=0))
        self.complete('cover_download')
        token = self.db.claim(self.vid, 'cover_upload', backoff=0)
        for bad in (object_path(self.vid), 'covers/' + str(uuid.uuid4()) + '/original.jpg', None):
            with self.assertRaises(Exception):
                self.db.call('finish', self.vid, 'cover_upload', token, {'cover_path': bad})
        self.assertIsNone(self.db.state(self.vid)['cover_path'])

    def test_absent_thumbnail_is_noted_and_closes_the_cover_track(self):
        self.complete('cover_download', {'cover_note': 'no_thumbnail_available'})
        state = self.db.state(self.vid)
        self.assertEqual(state['cover_note'], 'no_thumbnail_available')
        self.assertIsNone(state['cover_path'])
        # Nothing to upload: the stage is closed out rather than left pending.
        self.assertEqual(state['tasks']['cover_upload']['status'], 'succeeded')
        for stage in ('cover_download', 'cover_upload'):
            self.assertIsNone(self.db.claim(self.vid, stage, backoff=0))
        # The video track is entirely unaffected.
        self.uploaded()
        self.assertEqual(self.db.state(self.vid)['tasks']['ai']['status'], 'pending')

    def test_cover_note_must_be_a_code_and_a_real_cover_supersedes_it(self):
        token = self.db.claim(self.vid, 'cover_download', backoff=0)
        with self.assertRaises(Exception):
            self.db.call('finish', self.vid, 'cover_download', token, {'cover_note': 'Not A Code!'})
        self.expire('cover_download')  # The rejected finish left the claim running.
        self.owner.execute("UPDATE drama_archive.videos SET cover_note='no_thumbnail_available' WHERE id=%s", (self.vid,))
        # Clearing the note by hand is the documented way back for an ffmpeg fallback.
        self.owner.execute('UPDATE drama_archive.videos SET cover_note=NULL WHERE id=%s', (self.vid,))
        self.complete('cover_download')
        self.complete('cover_upload', {'cover_path': self.cover_path()})
        state = self.db.state(self.vid)
        self.assertEqual((state['cover_path'], state['cover_note']), (self.cover_path(), None))

    def test_committed_cover_is_not_claimable_again(self):
        self.complete('cover_download')
        self.complete('cover_upload', {'cover_path': self.cover_path()})
        self.owner.execute("UPDATE drama_archive.pipeline_tasks SET status='pending',attempts=0 "
                           "WHERE video_id=%s AND stage LIKE 'cover%%'", (self.vid,))
        for stage in ('cover_download', 'cover_upload'):
            self.assertIsNone(self.db.claim(self.vid, stage, backoff=0))

    def test_idempotent_import_and_media_identity_conflict(self):
        for _ in range(2):
            self.db.call('import_video', self.d, self.v)
        # download, upload, cover_download, cover_upload (pipeline_tasks) + the
        # drama-shared 'ai' task merged in from drama_tasks: still 5 keys.
        self.assertEqual(len(self.db.state(self.vid)['tasks']), 5)
        with self.assertRaises(Exception):
            self.db.call('import_video', self.d, {**self.v, 'media_id': '999'})
        self.assertEqual(self.db.state(self.vid)['media_id'], int(self.v['media_id']))
        with self.assertRaises(Exception):
            self.db.call('import_video', self.d, {**self.v, 'drama_id': str(uuid.uuid4())})

    def test_runtime_gate_rejects_owner_and_role_switch(self):
        for db in (Database(self.dsn), self.db):
            with self.assertRaisesRegex(PipelineError, 'restricted_worker_login_required'):
                db.verify_worker()
        self.assertTrue(self.worker.verify_worker()['restricted_login'])
        self.assertIsNotNone(self.worker.state(self.vid))

    def test_runtime_gate_allows_client_table_grants(self):
        self.owner.execute('GRANT SELECT(message_id) ON drama_archive.videos TO anon')
        try:
            self.assertTrue(self.worker.verify_worker()['restricted_login'])
        finally:
            self.owner.execute('REVOKE SELECT(message_id) ON drama_archive.videos FROM anon')

    def test_runtime_gate_detects_public_function_execute(self):
        self.owner.execute('GRANT EXECUTE ON FUNCTION drama_archive.video_state(uuid) TO PUBLIC')
        try:
            with self.assertRaises(PipelineError):
                self.worker.verify_worker()
        finally:
            self.owner.execute('REVOKE EXECUTE ON FUNCTION drama_archive.video_state(uuid) FROM PUBLIC')
        self.assertTrue(self.worker.verify_worker()['restricted_login'])

    def test_runtime_gate_detects_worker_direct_grants_and_disabled_rls(self):
        for change, undo in [
                ('GRANT SELECT ON drama_archive.videos TO drama_worker',
                 'REVOKE SELECT ON drama_archive.videos FROM drama_worker'),
                ('ALTER TABLE drama_archive.videos DISABLE ROW LEVEL SECURITY',
                 'ALTER TABLE drama_archive.videos ENABLE ROW LEVEL SECURITY')]:
            self.owner.execute(change)
            try:
                with self.assertRaises(PipelineError):
                    self.worker.verify_worker()
            finally:
                self.owner.execute(undo)

    def test_upload_lease_expires_while_waiting_for_video_lock(self):
        import psycopg
        self.complete('download')
        token = self.db.claim(self.vid, 'upload')
        # Hold the payload row but not the task row, so finish passes its initial check.
        blocker = psycopg.connect(self.dsn)
        try:
            blocker.execute('SELECT id FROM drama_archive.videos WHERE id=%s FOR UPDATE', (self.vid,))
            self.owner.execute("""UPDATE drama_archive.pipeline_tasks
                SET lease_expires_at=clock_timestamp()+interval '1 second'
                WHERE video_id=%s AND stage='upload'""", (self.vid,))
            with ThreadPoolExecutor(1) as pool:
                pending = pool.submit(self.db.call, 'finish', self.vid, 'upload', token,
                                      {'object_path': object_path(self.vid)})
                deadline = time.monotonic() + 3
                waiting = False
                while time.monotonic() < deadline:
                    waiting = bool(self.owner.execute("""SELECT count(*) FROM pg_stat_activity
                        WHERE datname='drama_pipeline_test' AND wait_event_type='Lock'
                          AND query LIKE 'SELECT drama_archive.finish%%'""").fetchone()[0])
                    if waiting:
                        break
                    time.sleep(0.01)
                time.sleep(1.1)
                blocker.rollback()
                self.assertTrue(waiting, 'finish must actually wait on the payload row')
                with self.assertRaises(psycopg.errors.RaiseException):
                    pending.result(timeout=5)
        finally:
            blocker.close()
        state = self.db.state(self.vid)
        self.assertIsNone(state['object_path'])
        self.assertEqual(state['tasks']['upload']['status'], 'running')

    def test_cross_source_fk_and_uniqueness(self):
        with self.assertRaises(Exception):
            self.db.call('import_video', self.d, {**self.v, 'source_id': '999'})
        d2, v2 = fixture(source=-100456)
        self.db.call('import_video', d2, v2)
        self.assertNotEqual(self.vid, v2['id'])

    def test_concurrent_claim_one_winner(self):
        with ThreadPoolExecutor(8) as pool:
            tokens = list(pool.map(lambda _: self.db.claim(self.vid, 'download'), range(8)))
        self.assertEqual(sum(t is not None for t in tokens), 1)

    def test_renew_expire_reclaim_old_worker_rejected(self):
        old = self.db.claim(self.vid, 'download')
        self.assertTrue(self.db.call('renew', self.vid, 'download', old, 120))
        self.expire()
        self.assertFalse(self.db.call('renew', self.vid, 'download', old, 120))
        new = self.db.claim(self.vid, 'download')
        self.assertNotEqual(old, new)
        with self.assertRaises(Exception):
            self.db.call('finish', self.vid, 'download', old, {})
        self.assertFalse(self.db.call('fail', self.vid, 'download', old, 'stale'))
        self.db.call('finish', self.vid, 'download', new, {})

    def test_expired_unreclaimed_finish_rejected(self):
        old = self.db.claim(self.vid, 'download')
        self.expire()
        with self.assertRaises(Exception):
            self.db.call('finish', self.vid, 'download', old, {})

    def test_stale_upload_and_ai_cannot_change_results(self):
        self.complete('download')
        old = self.db.claim(self.vid, 'upload')
        self.expire('upload')
        new = self.db.claim(self.vid, 'upload')
        with self.assertRaises(Exception):
            self.db.call('finish', self.vid, 'upload', old, {'object_path': object_path(self.vid)})
        self.assertIsNone(self.db.state(self.vid)['object_path'])
        self.db.call('finish', self.vid, 'upload', new, {'object_path': object_path(self.vid)})
        did = self.d['id']
        old = self.db.claim(did, 'ai')
        self.expire('ai')
        new = self.db.claim(did, 'ai')
        self.db.call('finish', did, 'ai', new,
                     {'title': 'new', 'tags': [], 'description': None, 'source_video_id': self.vid})
        with self.assertRaises(Exception):
            self.db.call('finish', did, 'ai', old,
                         {'title': 'old', 'tags': [], 'description': None, 'source_video_id': self.vid})
        self.assertEqual(self.db.state(self.vid)['ai']['title'], 'new')

    def test_retry_exhaustion_and_heartbeat(self):
        with Lease(self.db, self.vid, 'download', seconds=5) as lease:
            lease.fence()
            self.assertEqual(str(self.db.state(self.vid)['tasks']['download']['lease_token']), str(lease.token))
        self.expire()
        self.assertIsNone(self.db.claim(self.vid, 'download', attempts=1))

    def test_migration_conflict_does_not_replace_data(self):
        import psycopg
        with psycopg.connect(self.dsn) as conn:
            with self.assertRaises(psycopg.errors.DuplicateSchema):
                conn.execute((BASE / 'migrations/001_pipeline.sql').read_text())
        self.assertIsNotNone(self.db.state(self.vid))

    def test_migration_rejects_preexisting_role_members_and_privileges(self):
        import psycopg
        for grant in ('GRANT drama_worker TO anon', 'GRANT USAGE ON SCHEMA public TO drama_worker'):
            with psycopg.connect(self.dsn) as conn:
                conn.execute('DROP SCHEMA drama_archive CASCADE')
                conn.execute(psycopg.sql.SQL('REVOKE drama_worker FROM {}').format(
                    psycopg.sql.Identifier(self.login)))
                conn.execute(grant)
                with self.assertRaisesRegex(psycopg.errors.RaiseException, 'existing_worker_role_conflict'):
                    conn.execute((BASE / 'migrations/001_pipeline.sql').read_text())
                conn.rollback()
            self.assertIsNotNone(self.db.state(self.vid))

    def test_dependencies_and_retry_backoff(self):
        self.assertIsNone(self.db.claim(self.vid, 'upload'))
        self.assertIsNone(self.db.claim(self.d['id'], 'ai'))
        token = self.db.claim(self.vid, 'download')
        self.db.call('fail', self.vid, 'download', token, 'network_error')
        self.assertIsNone(self.db.claim(self.vid, 'download', backoff=3600))
        self.assertIsNotNone(self.db.claim(self.vid, 'download', backoff=0))

    def test_upload_and_ai_commit_are_atomic(self):
        self.complete('download')
        token = self.db.claim(self.vid, 'upload')
        with self.assertRaises(Exception):
            self.db.call('finish', self.vid, 'upload', token, {'object_path': 'wrong'})
        self.assertIsNone(self.db.state(self.vid)['object_path'])
        self.db.call('finish', self.vid, 'upload', token, {'object_path': object_path(self.vid)})
        self.assertEqual(self.db.state(self.vid)['tasks']['ai']['status'], 'pending')
        token = self.db.claim(self.d['id'], 'ai')
        with self.assertRaises(Exception):
            self.db.call('finish', self.d['id'], 'ai', token, {'tags': [1]})
        self.assertIsNone(self.db.state(self.vid)['ai'])

    def test_ai_failure_retains_previous_and_archive(self):
        self.uploaded()
        self.complete('ai', {'title': 'old', 'tags': ['a'], 'description': None})
        self.db.call('regenerate_ai', self.d['id'])
        token = self.db.claim(self.d['id'], 'ai')
        self.db.call('fail', self.d['id'], 'ai', token, 'provider_error')
        state = self.db.state(self.vid)
        self.assertEqual(state['ai']['title'], 'old')
        self.assertEqual(state['tasks']['upload']['status'], 'succeeded')
        self.assertIsNone(self.db.claim(self.vid, 'download'))

    def test_permissions_no_direct_tables_no_client_schema(self):
        import psycopg
        with psycopg.connect(self.db.dsn) as conn:
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                conn.execute('SELECT * FROM drama_archive.pipeline_tasks')
        self.assertFalse(self.owner.execute("SELECT has_schema_privilege('public','drama_archive','USAGE')").fetchone()[0])
        self.assertEqual(self.owner.execute("SELECT count(*) FROM pg_tables WHERE schemaname='drama_archive' AND rowsecurity").fetchone()[0], 5)
        self.assertEqual(self.owner.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='drama_archive'").fetchone()[0], 5)
        for role in ('anon', 'authenticated', 'service_role'):
            self.assertFalse(self.owner.execute("SELECT has_schema_privilege(%s,'drama_archive','USAGE')", (role,)).fetchone()[0])
            self.assertFalse(self.owner.execute("SELECT has_table_privilege(%s,'drama_archive.pipeline_tasks','SELECT')", (role,)).fetchone()[0])

    def test_error_check_rejects_sensitive_message(self):
        token = self.db.claim(self.vid, 'download')
        with self.assertRaises(Exception):
            self.db.call('fail', self.vid, 'download', token, 'secret https://signed?token=secret')
        self.assertIsNone(self.db.state(self.vid)['tasks']['download']['last_error'])

    def test_upload_db_failure_restart_reconciles_without_reupload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = Store(root)
            r = record_message(message(6558), SOURCE)
            # Match local source and imported identity.
            d, v = fixture(source=SOURCE['id'])
            self.db.call('import_video', d, v)
            p = root / 'videos' / filename(r)
            p.write_bytes(b'test-video')
            digest = hashlib.sha256(p.read_bytes()).hexdigest()
            r['expected_size'] = p.stat().st_size
            s = {'status': 'success', 'local_path': str(p.relative_to(root)), 'sha256': digest}
            cache, storage = Cache(root), TusDouble()
            validated = {'actual_size': p.stat().st_size, 'sha256': digest}
            original = self.db.call
            def fail_commit(name, *args):
                if name == 'finish' and args[1] == 'upload':
                    raise OSError('commit unavailable')
                return original(name, *args)
            with patch('pipeline.validate_file', return_value=validated), patch.object(self.db, 'call', side_effect=fail_commit):
                with self.assertRaises(OSError):
                    archive_existing(self.db, storage, cache, v, r, s)
            self.assertTrue(p.exists())
            self.assertIsNotNone(storage.content)
            self.owner.execute("UPDATE drama_archive.pipeline_tasks SET updated_at=clock_timestamp()-interval '1 hour' WHERE video_id=%s", (v['id'],))
            with patch('pipeline.validate_file', return_value=validated):
                archive_existing(Database(self.db.dsn), storage, Cache(root), v, r, s)
            self.assertEqual(storage.posts, 1)
            self.assertTrue(cache.get(v['id'])['archived'])
            evidence = sample_evidence(self.db, cache, v, r)
            self.assertEqual(evidence['local_sha256'], evidence['remote_sha256'])
            self.assertEqual(evidence['ai_status'], 'pending')
            p.unlink()  # Test data only; not real protected samples.
            with cache.connect() as journal:
                journal.execute('DELETE FROM entries')
            self.assertEqual(archive_existing(self.db, storage, cache, v, r, s), 'already_archived_readback_verified')
            self.assertEqual(storage.posts, 1)
            self.assertTrue(cache.get(v['id'])['protected'])
            self.assertEqual(sample_evidence(self.db, cache, v, r)['remote_sha256'], digest)
            with self.assertRaisesRegex(PipelineError, 'identity_conflict'):
                archive_existing(self.db, storage, cache, {**v, 'media_id': '999'}, r, s)
            store.db.close()

    def test_ai_test_double_guard_and_cloud_input(self):
        self.uploaded()
        with tempfile.TemporaryDirectory() as tmp:
            cache = Cache(tmp)
            storage = TusDouble()
            storage.content = b'video'
            cache.put(self.vid, size=5, sha256=hashlib.sha256(b'video').hexdigest())
            provider = Mock(is_test_double=True)
            provider.analyze.return_value = {'title': 'test-only', 'tags': [], 'description': None}
            with self.assertRaisesRegex(PipelineError, 'forbidden'):
                run_ai(self.db, storage, cache, self.d['id'], provider)
            run_ai(self.db, storage, cache, self.d['id'], provider, allow_test_provider=True)
            self.assertEqual(self.db.state(self.vid)['ai']['title'], 'test-only')
            self.assertFalse(any((cache.directory / 'scratch').iterdir()))

    def test_ai_candidates_and_run_ai_extract_frames_and_commit(self):
        import subprocess
        from core import sha256
        from pipeline_ai import Provider
        from pipeline_batch import select_ai_candidates
        self.uploaded()
        with tempfile.TemporaryDirectory() as tmp:
            video_path = Path(tmp) / 'sample.mp4'
            subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                            'color=c=blue:s=64x64:d=1.2', '-c:v', 'mpeg4', '-y', str(video_path)], check=True)
            cache = Cache(tmp)
            cache.put(self.vid, path=str(video_path), size=video_path.stat().st_size,
                      sha256=sha256(video_path))
            rows = [(self.d, self.v, {'message_id': int(self.v['message_id'])}, {})]
            candidates = select_ai_candidates(rows, self.db.archived_messages())
            self.assertEqual([v['id'] for _, v, _, _ in candidates], [self.vid])
            provider = Provider({'frame_count': 2, 'max_dim': 64})
            with patch('pipeline_ai.call_model', return_value={
                    'title': 'drama title', 'tags': ['a'], 'description': None}) as called:
                run_ai(self.db, None, cache, self.d['id'], provider)
            self.assertEqual(len(called.call_args[0][0]), 2)
            self.assertEqual(self.db.state(self.vid)['ai']['title'], 'drama title')

    def test_bounded_download_uses_existing_identity_and_validation(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            source_file = Path(tmp) / 'fixture.mp4'
            subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                            'color=c=blue:s=64x64:d=0.3', '-c:v', 'mpeg4', '-y', str(source_file)], check=True)
            content = source_file.read_bytes()
            msg = message(6558, len(content))
            r = record_message(msg, SOURCE)
            store.ingest([r], 6559, 6558, [6558])
            state = store.videos()[0][1]
            d, v = fixture(source=SOURCE['id'])
            self.db.call('import_video', d, v)
            client = FakeClient([msg], content)
            # A previous test project tombstone must not block an uncommitted target project.
            Cache(tmp).put(v['id'], archived=True, object_path=object_path(v['id']), sha256='old', remote_sha256='old')
            cache = Cache(tmp, project_ref='b' * 20)
            result = asyncio.run(download_task(self.db, cache, v, r, state, client, None, SOURCE, store))
            self.assertEqual(result, 'downloaded_success')
            self.assertEqual(self.db.state(v['id'])['tasks']['download']['status'], 'succeeded')
            self.assertEqual(client.downloads, 1)
            store.db.close()

    def test_connection_retry_is_bounded_and_only_for_connect(self):
        import psycopg
        calls = []
        def flaky(*args, **kwargs):
            calls.append(1)
            if len(calls) < 3:
                raise psycopg.OperationalError('refused')
            return 'connection'
        with patch('psycopg.connect', flaky), patch('time.sleep'):
            self.assertEqual(Database('x').connect(), 'connection')
        self.assertEqual(len(calls), 3)
        calls.clear()
        with patch('psycopg.connect', Mock(side_effect=psycopg.OperationalError('refused'))) as always, \
                patch('time.sleep'), self.assertRaises(psycopg.OperationalError):
            Database('x').connect()
        self.assertEqual(always.call_count, 4)
        import re
        code = error_code(psycopg.OperationalError('password=secret'))
        self.assertEqual(code, 'psycopg_OperationalError')
        self.assertRegex(code, r'^[A-Za-z0-9_]{1,80}$')  # matches the last_error CHECK

    def test_archived_messages_function_is_restricted_and_reports_commits(self):
        self.assertEqual(self.db.archived_messages(), set())
        self.uploaded()
        self.assertEqual(self.db.archived_messages(), {int(self.v['message_id'])})
        for role in ('anon', 'authenticated', 'service_role'):
            if self.owner.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (role,)).fetchone():
                self.assertFalse(self.owner.execute(
                    "SELECT has_function_privilege(%s,'drama_archive.archived_messages()','EXECUTE')",
                    (role,)).fetchone()[0])
        self.assertTrue(self.worker.verify_worker()['restricted_login'])

    def test_batch_select_order_and_limits(self):
        from pipeline_batch import select
        rows = [(None, None, {'message_id': i}, {'status': st}) for i, st in
                [(5, 'pending'), (3, 'success'), (9, 'pending'), (7, 'failed'), (8, 'blocked'), (2, 'pending')]]
        self.assertEqual([r[2]['message_id'] for r in select(rows, 3, [9])], [9, 2, 3])
        for bad in (0, 101, True):
            with self.assertRaises(PipelineError):
                select(rows, bad)
        with self.assertRaisesRegex(PipelineError, 'first_id_not_pending'):
            select(rows, 2, [8])
        # Local success whose upload never committed is picked up; archived ones are not.
        self.assertEqual([r[2]['message_id'] for r in select(rows, 4, [3])], [3, 2, 5, 7])
        self.assertEqual([r[2]['message_id'] for r in select(rows, 4, [], {2, 5})], [3, 7, 9])

    def test_batch_concurrent_download_upload_flood_and_cleanup(self):
        import subprocess
        from pipeline_batch import process_batch, select
        class MemStorage:
            # Keyed by bucket and object path: the cover must not see the video's object.
            objects, posts, cover_posts = {}, 0, 0
            def readback(self, vid, size, check=lambda: None, output=None, *,
                         bucket='drama-videos', path=None, **options):
                data = self.objects.get((bucket, path or object_path(vid)))
                return None if data is None else hashlib.sha256(data).hexdigest()
            def upload(self, vid, path, sha, cache, check, mime, **options):
                self.posts += 1
                self.objects['drama-videos', object_path(vid)] = path.read_bytes()
                return self.readback(vid, 0)
            def upload_cover(self, vid, path, sha, check=lambda: None):
                self.cover_posts += 1
                self.objects[COVERS, cover_object_path(vid)] = path.read_bytes()
                return self.readback(vid, 0, bucket=COVERS, path=cover_object_path(vid))
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            src = Path(tmp) / 'fixture.mp4'
            subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                            'color=c=blue:s=64x64:d=0.3', '-c:v', 'mpeg4', '-y', str(src)], check=True)
            content = src.read_bytes()
            src.unlink()
            msgs = [message(i, len(content), name=f'{i}.mp4') for i in (101, 102, 103, 104)]
            recs = [record_message(m, SOURCE) for m in msgs]
            store.ingest(recs, 105, 101, [m.id for m in msgs])
            states = {r['message_id']: s for r, s in store.videos()}
            jobs = []
            for r in recs:
                d, v = fixture(r['message_id'], source=SOURCE['id'])
                v['media_id'] = str(r['media_id'])
                jobs.append((d, v, r, states[r['message_id']]))
            client = FakeClient(msgs, content)
            storage, cache = MemStorage(), Cache(tmp)
            report_path = Path(tmp) / 'batch.json'
            real_download = client.download_media
            async def flooding(msg, file, progress_callback=None, thumb=None):
                if msg.id == 101 and not client.flooded and thumb is None:
                    client.flooded = True
                    archive.on_flood(7)
                await real_download(msg, file, progress_callback, thumb)
            client.flooded = False
            client.download_media = flooding
            report = asyncio.run(process_batch(self.db, storage, cache, store, client, None, SOURCE, jobs,
                                               concurrency=2, cleanup=True, report_path=report_path,
                                               project_ref=PROJECT))
            self.assertEqual(report['status'], 'all_archived', report['videos'])
            self.assertLessEqual(report['peak_downloads'], 2)
            self.assertEqual(report['download_limit_now'], 1)
            self.assertEqual(len(report['flood_events']), 1)
            self.assertEqual(storage.posts, 4)
            self.assertIsNone(archive.on_flood)
            self.assertEqual(report['cover_counts'], {'cover_uploaded_readback_verified': 4})
            self.assertLessEqual(report['peak_covers'], 2)
            self.assertEqual(storage.cover_posts, 4)
            for _, v, r, _ in jobs:
                cloud = self.db.state(v['id'])
                self.assertEqual(cloud['object_path'], object_path(v['id']))
                self.assertEqual(cloud['cover_path'], cover_object_path(v['id']))
                self.assertEqual(cloud['tasks']['ai']['status'], 'pending')
                self.assertFalse((Path(tmp) / 'videos' / filename(r)).exists())
                # Cleanup removes the video; the cover it was archived with stays.
                self.assertTrue((Path(tmp) / 'covers' / (filename(r) + '.jpg')).exists())
            self.assertEqual(covers_committed(tmp, PROJECT), {m.id for m in msgs})
            # Evidence is per cloud project: another project has committed nothing here.
            self.assertEqual(covers_committed(tmp, 'b' * 20), set())
            with self.assertRaisesRegex(PipelineError, 'cover_project_ref_required'):
                covers_committed(tmp, None)
            # Rerun: nothing downloaded or uploaded again, on either track.
            report = asyncio.run(process_batch(self.db, storage, cache, store, client, None, SOURCE, jobs,
                                               concurrency=2, report_path=report_path,
                                               project_ref=PROJECT))
            self.assertEqual((client.downloads, storage.posts), (4, 4))
            self.assertEqual((client.thumb_downloads, storage.cover_posts), (4, 4))
            self.assertEqual(report['cover_counts'], {'already_archived': 4})
            self.assertEqual(report['status'], 'all_archived')
            store.db.close()

    @unittest.skipUnless((BASE / 'archive/state.sqlite3').exists(), 'optional existing local inventory')
    def test_full_existing_inventory_import_twice(self):
        # Test only the real local metadata against the isolated local DB, no media/network.
        rows, _ = snapshot(BASE / 'archive', BASE / 'archive/topics-20261019-184029-928673/topics.json')
        from psycopg.types.json import Jsonb
        import psycopg
        with psycopg.connect(self.db.dsn) as conn:
            for _ in range(2):
                with conn.cursor() as cursor:
                    cursor.executemany('SELECT drama_archive.import_video(%s,%s)',
                                       [(Jsonb(d), Jsonb(v)) for d, v, _, _ in rows])
        count = self.owner.execute('SELECT count(*) FROM drama_archive.videos').fetchone()[0]
        self.assertEqual(count, len(rows) + 1)


if __name__ == '__main__':
    unittest.main()
