"""Scheduling, cancellation and project boundaries without real network or credentials."""
import asyncio
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.core import StopArchive, Store, filename, record_message
from video_archive.pipeline.batch import (_plan_files as plan, process_batch, process_drama_pass,
                                          retry_plan_dramas, plan_dramas, claim_blocker)
from video_archive.screening.quality import QualityPolicy
from video_archive.pipeline.cache import Cache
from video_archive.pipeline.common import PipelineError
from test_pipeline import fixture
from test_archive import FakeClient, SOURCE, message


def jobs(count=3, *, separate_dramas=False):
    result = []
    for mid in range(101, 101 + count):
        drama, video = fixture(mid, topic=mid if separate_dramas else 20)
        record = {'message_id': mid, 'expected_size': 10, 'source': {'id': -100123},
                  'media_id': 9000 + mid, 'original_filename': 'fixture.mp4',
                  'width': 720, 'height': 1280, 'bitrate_bps': 2_000_000}
        result.append((drama, video, record, {'status': 'pending'}))
    return result


class SchedulingTests(unittest.TestCase):
    def test_retry_is_project_scoped_latest_evidence_only_and_deferred_is_explicit(self):
        rows = jobs(3, separate_dramas=True)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'report.json'
            done = 'cover_uploaded_readback_verified'
            report = {'project_ref': 'a'*20, 'segments': [
                {'videos': [{'message_id': 101, 'result': 'failed', 'cover_result': done},
                            {'message_id': 102, 'result': 'downloading', 'cover_result': 'failed'}]},
                {'videos': [{'message_id': 101, 'result': 'uploaded_readback_verified',
                             'cover_result': done}]}],
                'deferred': [{'message_id': 103, 'reason': 'large_video'}]}
            path.write_text(json.dumps(report))
            selected, _, _ = retry_plan_dramas(rows, tmp, path, 'a'*20, quality_policy=QualityPolicy())
            self.assertEqual([j[2]['message_id'] for j in selected], [102])
            selected, _, _ = retry_plan_dramas(rows, tmp, path, 'a'*20, quality_policy=QualityPolicy(), include_deferred=True)
            self.assertEqual([j[2]['message_id'] for j in selected], [102, 103])
            with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
                retry_plan_dramas(rows, tmp, path, 'b'*20, quality_policy=QualityPolicy())

    def test_cover_failure_alone_reselects_an_archived_video(self):
        """The two tracks retry independently: a committed video with no cover is work."""
        rows = jobs(2, separate_dramas=True)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'report.json'
            path.write_text(json.dumps({'project_ref': 'a'*20, 'segments': [{'videos': [
                {'message_id': 101, 'result': 'uploaded_readback_verified', 'cover_result': 'failed'},
                {'message_id': 102, 'result': 'uploaded_readback_verified',
                 'cover_result': 'cover_uploaded_readback_verified'}]}]}))
            selected, _, _ = retry_plan_dramas(rows, tmp, path, 'a'*20, quality_policy=QualityPolicy())
            self.assertEqual([j[2]['message_id'] for j in selected], [101])

    def test_plan_keeps_archived_videos_whose_cover_is_outstanding(self):
        rows = jobs(3)
        with tempfile.TemporaryDirectory() as tmp:
            selected, _ = plan(rows, tmp, {101, 102, 103}, covered={101, 103})
            self.assertEqual([j[2]['message_id'] for j in selected], [102])
            selected, _ = plan(rows, tmp, {101, 102, 103}, covered={101, 102, 103})
            self.assertEqual(selected, [])

    def test_committed_video_cover_is_not_stopped_by_the_video_size_limit(self):
        """A 900 MB video over the bucket limit still yields its tiny cover."""
        rows = jobs(2)
        rows[0][2]['expected_size'] = 915 * 1024**2
        with tempfile.TemporaryDirectory() as tmp:
            selected, deferred = plan(rows, tmp, {101, 102}, covered={102},
                                      cover_only={101, 102}, bucket_limit=512 * 1024**2)
            self.assertEqual([j[2]['message_id'] for j in selected], [101])
            self.assertEqual(deferred, [])
            # Without the cover-only flag it stays deferred: the video really is too large.
            selected, deferred = plan(rows, tmp, {101, 102}, covered={102},
                                      bucket_limit=512 * 1024**2)
            self.assertEqual([j[2]['message_id'] for j in selected], [])
            self.assertEqual(deferred[0]['reason'], 'bucket_file_limit')

    def test_recovery_selection_includes_unstarted_later_segments(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'report.json'
            path.write_text(json.dumps({'project_ref': 'a'*20, 'segments': [],
                                        'pending_message_ids': [101, 102, 103]}))
            selected, _, _ = retry_plan_dramas(jobs(), tmp, path, 'a'*20, quality_policy=QualityPolicy())
            self.assertEqual(len(selected), 3)

    def test_db_attempt_backoff_and_active_lease_are_not_forced(self):
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        task = {'status': 'failed', 'attempts': 1, 'updated_at': now.isoformat()}
        cloud = {'tasks': {'download': task}}
        self.assertEqual(claim_blocker(cloud), 'database_backoff')
        task['updated_at'] = (now - timedelta(seconds=31)).isoformat()
        self.assertIsNone(claim_blocker(cloud))
        task.update(status='running', lease_expires_at=(now+timedelta(seconds=60)).isoformat())
        self.assertEqual(claim_blocker(cloud), 'lease_active')
        task['lease_expires_at'] = (now-timedelta(seconds=1)).isoformat()
        self.assertIsNone(claim_blocker(cloud))
        task['attempts'] = 5
        self.assertEqual(claim_blocker(cloud), 'attempts_exhausted')

    def test_partial_resume_appends_working_copy_preserving_original(self):
        import video_archive.telegram.archive as archive
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            msg = message(3021)
            record = record_message(msg, SOURCE)
            store.ingest([record], 3022, 3021, [3021])
            state = store.videos()[0][1]
            partial = Path(tmp) / 'videos' / (filename(record) + '.part')
            partial.write_bytes(b'0123')
            offsets = []
            class Stream:
                def __aiter__(self):
                    async def chunks():
                        yield b'456'; yield b'789'
                    return chunks()
                async def close(self): pass
            client = FakeClient([msg])
            def iterator(media, **kwargs):
                offsets.append(kwargs['offset']); return Stream()
            client.iter_download = iterator
            try:
                with patch('video_archive.telegram.archive.validate_file', return_value={'actual_size': 10, 'sha256': 'test', 'decoded_at': None}):
                    result = asyncio.run(archive.download_one(client, None, SOURCE, store, record, state,
                                                             resume_partial=True, preserve_partial=True, retries=1))
                self.assertEqual(result, 'downloaded_success')
                self.assertEqual(offsets, [4])
                self.assertEqual(partial.read_bytes(), b'0123')
                self.assertEqual(partial.with_name(filename(record)).read_bytes(), b'0123456789')
            finally:
                store.db.close()

    def test_async_lease_claim_does_not_block_event_loop(self):
        from video_archive.pipeline.db import Lease
        entered = threading.Event()
        def claim(*args):
            time.sleep(0.08); entered.set(); return 'test-token'
        db = SimpleNamespace(claim=claim)
        ticks = []
        async def run():
            async def ticker():
                while not entered.is_set():
                    ticks.append(1); await asyncio.sleep(0.005)
            tick = asyncio.create_task(ticker())
            async with Lease(db, 'test', 'download'):
                self.assertGreater(len(ticks), 4)
            await tick
        asyncio.run(run())

    def test_inflight_imports_are_bounded_and_file_errors_continue(self):
        rows = jobs(12)
        store = SimpleNamespace(videos=lambda: [(j[2], j[3]) for j in rows])
        active, peak = 0, 0
        guard = threading.Lock()
        def call(*args):
            nonlocal active, peak
            with guard:
                active += 1; peak = max(peak, active)
            time.sleep(0.01)
            with guard: active -= 1
            raise ValueError('single-file-fixture')
        db = SimpleNamespace(call=call)
        report = asyncio.run(process_batch(db, None, None, store, None, None, None, rows, concurrency=4))
        self.assertLessEqual(peak, 4)
        self.assertEqual(report['counts'], {'failed': 12})
        self.assertIsNone(report['stop_reason'])

    def test_retry_sweep_starts_after_normal_queue_and_is_segmented_once(self):
        calls = []
        async def segment(*args, **kwargs):
            mids = [j[2]['message_id'] for j in args[7]]
            retrying = kwargs.get('resume_partials', False)
            calls.append((mids, retrying))
            return {'videos': [{'message_id': mid,
                                'result': 'uploaded_readback_verified' if retrying else 'failed',
                                'cover_result': 'no_thumbnail'} for mid in mids],
                    'download_limit_now': 4, 'stop_reason': None}
        async def no_delay(*args): pass
        async def run():
            with tempfile.TemporaryDirectory() as tmp:
                rows = jobs(5)
                selected, _, evaluations = plan_dramas(rows, tmp, set(), quality_policy=QualityPolicy())
                with patch('video_archive.pipeline.batch.process_batch', segment), patch('video_archive.pipeline.batch.asyncio.sleep', no_delay):
                    report = await process_drama_pass(*([None]*7), selected, drama_evaluations=evaluations,
                                                      batch_size=2, retry_rounds=1,
                                                      deferred=[{'message_id': 3021}])
            self.assertEqual(calls, [([101,102],False),([103,104],False),([105],False),
                                     ([101,102],True),([103,104],True),([105],True)])
            self.assertEqual(report['status'], 'normal_queue_archived')
            self.assertEqual(report['counts'], {'uploaded_readback_verified': 5})
        asyncio.run(run())

    def test_timeout_keeps_partial_and_next_pass_does_not_delete_or_restart_it(self):
        import video_archive.telegram.archive as archive
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(tmp)
            msg = message(3021)
            record = record_message(msg, SOURCE)
            store.ingest([record], 3022, 3021, [3021])
            state = store.videos()[0][1]
            client = FakeClient([msg])
            async def slow(msg, file, progress_callback):
                client.downloads += 1
                file.write(b'keep')
                await asyncio.sleep(1)
            client.download_media = slow
            options = dict(timeout_seconds=0.02, retries=1, preserve_partial=True)
            try:
                result = asyncio.run(archive.download_one(client, None, SOURCE, store, record, state, **options))
                self.assertEqual(result, 'failed')
                partial = Path(tmp) / 'videos' / (filename(record) + '.part')
                self.assertEqual(partial.read_bytes(), b'keep')
                with self.assertRaisesRegex(StopArchive, 'partial_preserved'):
                    asyncio.run(archive.download_one(client, None, SOURCE, store, record, state, **options))
                self.assertEqual(client.downloads, 1)
                self.assertEqual(partial.read_bytes(), b'keep')
            finally:
                store.db.close()

    def test_project_caches_do_not_reuse_tombstones_or_upload_sessions(self):
        with tempfile.TemporaryDirectory() as tmp:
            vid = fixture()[1]['id']
            legacy, test, main = Cache(tmp), Cache(tmp, project_ref='a' * 20), Cache(tmp, project_ref='b' * 20)
            legacy.put(vid, archived=True, upload_url='legacy-private-session')
            test.put(vid, archived=True, upload_url='test-private-session')
            self.assertEqual(main.get(vid), {})
            self.assertEqual(test.get(vid)['upload_url'], 'test-private-session')
            self.assertEqual(legacy.get(vid)['upload_url'], 'legacy-private-session')
            with self.assertRaises(PipelineError):
                Cache(tmp, project_ref='../escape')

    def test_plan_defers_large_partial_and_protected_id_but_retries_local_success(self):
        rows = jobs(5)
        rows[0][3]['status'] = 'success'  # Local success alone is not a cloud commit.
        rows[1][2]['expected_size'] = 500
        rows[4][2]['expected_size'] = 2000
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / 'videos'
            directory.mkdir()
            part = directory / (filename(rows[2][2]) + '.part')
            part.write_bytes(b'preserve')
            chosen, deferred = plan(rows, tmp, set(), max_bytes=100, bucket_limit=1000, defer_ids=[104])
            self.assertEqual([x[2]['message_id'] for x in chosen], [101])
            self.assertEqual({x['reason'] for x in deferred},
                             {'large_video', 'bucket_file_limit', 'existing_partial_preserved', 'explicitly_deferred'})
            self.assertEqual(part.read_bytes(), b'preserve')
            chosen, _ = plan(rows, tmp, {101}, only=[101], max_bytes=100)
            self.assertEqual(len(chosen), 1)  # Explicit repeat verification is allowed.

    def test_long_flood_stops_already_queued_downloads(self):
        import video_archive.telegram.archive as archive
        rows = jobs()
        store = SimpleNamespace(videos=lambda: [(x[2], x[3]) for x in rows])
        db = SimpleNamespace(call=lambda *a: None, state=lambda *a: {
            'tasks': {'upload': {'status': 'pending'}, 'download': {'status': 'pending'}}})
        started = []
        async def download(*args, **kwargs):
            started.append(args[2]['message_id'])
            await asyncio.sleep(0.03)  # Let other tasks queue behind the gate.
            archive.on_flood(61)
            raise StopArchive('flood_wait: 61s')
        with patch('video_archive.pipeline.batch.download_task', download):
            report = asyncio.run(process_batch(db, None, None, store, None, None, None, rows, concurrency=1))
        self.assertEqual(len(started), 1)
        self.assertEqual(report['stop_reason'], 'flood_wait')
        self.assertEqual(sum(x['result'] == 'not_started' for x in report['videos']), 2)

    def test_cancellation_waits_for_upload_thread_to_exit(self):
        rows = jobs(1)
        store = SimpleNamespace(videos=lambda: [(x[2], x[3]) for x in rows])
        db = SimpleNamespace(call=lambda *a: None, state=lambda *a: {
            'tasks': {'upload': {'status': 'pending'}, 'download': {'status': 'succeeded'}}})
        entered, exited = threading.Event(), threading.Event()
        def upload(*args, check, **kwargs):
            entered.set()
            try:
                while True:
                    check()
                    time.sleep(0.005)
            finally:
                time.sleep(0.03)
                exited.set()
        async def run():
            task = asyncio.create_task(process_batch(db, None, None, store, None, None, None, rows))
            for _ in range(200):
                if entered.is_set():
                    break
                await asyncio.sleep(0.005)
            self.assertTrue(entered.is_set())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(exited.is_set())
        with patch('video_archive.pipeline.batch.archive_existing', upload):
            asyncio.run(run())

    def test_finite_pass_advances_after_failure_but_stops_on_global_stop(self):
        calls = []
        async def segment(*args, **kwargs):
            batch = args[7]
            calls.extend(x[2]['message_id'] for x in batch)
            return {'videos': [{'message_id': x[2]['message_id'], 'result': 'failed',
                                'cover_result': 'no_thumbnail'} for x in batch],
                    'download_limit_now': 1, 'stop_reason': None}
        async def run():
            with tempfile.TemporaryDirectory() as tmp:
                selected, _, evaluations = plan_dramas(jobs(), tmp, set(), quality_policy=QualityPolicy())
                with patch('video_archive.pipeline.batch.process_batch', segment):
                    report = await process_drama_pass(*([None] * 7), selected,
                                                      drama_evaluations=evaluations, batch_size=1)
                self.assertEqual(report['status'], 'incomplete')
                self.assertEqual(calls, [101, 102, 103])
                stopped = AsyncMock(return_value={'videos': [], 'download_limit_now': 1, 'stop_reason': 'flood_wait'})
                with patch('video_archive.pipeline.batch.process_batch', stopped):
                    report = await process_drama_pass(*([None] * 7), selected,
                                                      drama_evaluations=evaluations, batch_size=1)
                self.assertEqual(report['status'], 'stopped')
                self.assertEqual(stopped.await_count, 1)
        asyncio.run(run())


if __name__ == '__main__':
    unittest.main()
