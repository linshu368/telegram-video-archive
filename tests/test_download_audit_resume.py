"""Resume real verified files and partial bytes; stop on exhausted connection recovery."""
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from scripts.diagnostics import audit_download_completeness as audit
from video_archive.core import Store, StopArchive, filename, record_message, validate_file
from video_archive.telegram import archive
from test_archive import FakeClient, SOURCE, message


class DownloadAuditResumeTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.media = tempfile.TemporaryDirectory()
        path = Path(cls.media.name) / 'fixture.mp4'
        subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                        'color=c=blue:s=64x64:d=0.3', '-c:v', 'mpeg4', '-y', str(path)], check=True)
        cls.content = path.read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls.media.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store = Store(self.root)
        self.store.cloud_state_checked = True
        self.messages = {}
        self.records = {}
        self.args = SimpleNamespace(session='download-audit', metadata_only=False, concurrency=4)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def seed(self, *, include_metadata=True):
        dramas, outcomes = [], {}
        for tid, mids in [(10, [101]), (20, [201, 202])] + ([(30, [301])] if include_metadata else []):
            records = []
            for mid in mids:
                msg = message(mid, size=len(self.content))
                self.messages[mid] = msg
                record = record_message(msg, SOURCE)
                record['topic_id'] = tid
                self.records[mid] = record
                records.append(record)
            dramas.append({'topic_id': tid, 'topic_title': str(tid), 'records': records})
            if tid == 30:
                outcomes[str(tid)] = {'topic_id': tid, 'status': 'metadata_failed',
                                      'error': 'ConnectionError', 'videos': []}
                continue
            self.store.ingest_topic_records(records)
            entries = []
            for record, state in self.store.videos():
                if record['topic_id'] != tid:
                    continue
                mid = record['message_id']
                if mid == 202:
                    state.update(status='failed', attempts=5, failure_reason='ConnectionError')
                    state['events'] = [{'event': 'attempt_failed', 'reason': 'ConnectionError'} for _ in range(5)]
                    history = [{'success': False, 'failure_reason': 'ConnectionError'} for _ in range(5)]
                    partial = self.root / 'videos' / (filename(record) + '.part.resume')
                    partial.write_bytes(self.content[:len(self.content) // 2])
                else:
                    path = self.root / 'videos' / filename(record)
                    path.write_bytes(self.content)
                    state.update(validate_file(path, len(self.content)), status='success', attempts=1)
                    state['events'] = [{'event': 'download_verified', 'reason': ''}]
                    history = [{'success': True, 'result': 'downloaded_success'}]
                self.store.update(mid, state)
                entries.append({'message_id': mid, 'media_id': record['media_id'],
                                'expected_size': record['expected_size'], 'success': mid != 202,
                                'failure_reason': state.get('failure_reason'), 'history': history,
                                'actual_size': state.get('actual_size')})
            outcomes[str(tid)] = {'topic_id': tid, 'status': 'finished', 'metadata_complete': True,
                                  'expected_message_ids': mids, 'expected_count': len(mids),
                                  'videos': entries, 'outcome': 'all_success' if tid == 10 else 'partial_failure',
                                  'delivery_decision': 'complete' if tid == 10 else 'rejected_incomplete',
                                  'first_pass_failed_count': int(tid == 20),
                                  'success_count': 1, 'failure_count': int(tid == 20)}
        plan = {'source': SOURCE, 'source_boundary': {'upper_id': 1000,
                 'cutoff_exclusive_utc': '2026-10-20T00:00:00+00:00'},
                'population_drama_count': 891, 'requested_drama_count': len(dramas), 'dramas': dramas}
        report = {'status': 'finished', 'media_retention': 'keep_all', 'dramas': outcomes}
        (self.root / 'selection.json').write_text(json.dumps(plan))
        audit.save(self.root, plan, report)
        return plan, report

    def client(self):
        client = FakeClient(list(self.messages.values()))
        client.connect = AsyncMock()
        client.disconnect = AsyncMock()
        client.is_user_authorized = AsyncMock(return_value=True)
        client.is_bot = AsyncMock(return_value=False)
        client.is_connected = lambda: True
        return client

    async def run_audit(self, client, plan, report):
        with (patch.object(audit, 'load_config', return_value={}),
              patch.object(audit, 'client_for', return_value=client) as factory,
              patch.object(audit, 'confirmed_target', new=AsyncMock(return_value=(None, SOURCE)))):
            await audit.execute(self.args, self.root, plan, report, self.store)
        factory.assert_called_once_with({}, session_name='download-audit', auto_reconnect=True)

    async def test_resume_reopens_only_connection_failures_and_preserves_raw_history(self):
        plan, report = self.seed()
        original = (self.root / 'report.json').read_bytes()
        partial = self.root / 'videos' / (filename(self.records[202]) + '.part.resume')
        old_partial = partial.read_bytes()
        audit.resume_existing(self.root, report, self.store)
        self.assertEqual(report['dramas']['10']['status'], 'finished')
        self.assertEqual(report['dramas']['20']['status'], 'metadata_ready')
        self.assertEqual(report['dramas']['30']['status'], 'queued')
        self.assertEqual(report['resume']['verified_existing_video_count'], 2)
        self.assertEqual(report['resume']['requeued_video_count'], 1)
        self.assertEqual(report['resume']['requeued_metadata_drama_count'], 1)
        snapshot = Path(report['resume']['snapshot'])
        self.assertEqual((snapshot / 'report.json').read_bytes(), original)
        self.assertEqual(partial.read_bytes(), old_partial)
        record, state = next(row for row in self.store.videos() if row[0]['message_id'] == 202)
        entry = next(v for v in report['dramas']['20']['videos'] if v['message_id'] == 202)
        self.assertEqual(state['attempts'], 5)
        self.assertEqual(len(entry['history']), 5)
        self.assertEqual(audit.effective_attempts(entry, state), 0)
        self.assertEqual(audit.summarize(plan, report['dramas'])['finished_drama_count'], 1)

    async def test_real_partial_appends_without_redownloading_successful_files(self):
        plan, report = self.seed(include_metadata=False)
        plan_bytes = (self.root / 'selection.json').read_bytes()
        unchanged = {mid: (self.root / 'videos' / filename(self.records[mid])).stat().st_mtime_ns
                     for mid in (101, 201)}
        audit.resume_existing(self.root, report, self.store)
        client = self.client()
        offsets = []

        class Stream:
            def __init__(inner, offset):
                inner.offset = offset

            async def __aiter__(inner):
                yield self.content[inner.offset:]

            async def close(inner):
                pass

        def iterator(media, *, offset, **kwargs):
            offsets.append(offset)
            return Stream(offset)

        client.iter_download = iterator
        await self.run_audit(client, plan, report)
        self.assertEqual(offsets, [len(self.content) // 2])
        self.assertEqual(client.downloads, 0)
        self.assertEqual((self.root / 'videos' / filename(self.records[202])).read_bytes(), self.content)
        for mid, mtime in unchanged.items():
            self.assertEqual((self.root / 'videos' / filename(self.records[mid])).stat().st_mtime_ns, mtime)
        self.assertEqual((self.root / 'selection.json').read_bytes(), plan_bytes)
        summary = report['summary']
        self.assertEqual((summary['finished_drama_count'], summary['evaluated_video_count']), (2, 3))
        self.assertEqual(summary['final_failed_video_count'], 0)
        self.assertEqual(summary['recovered_on_retry_video_count'], 1)
        self.assertEqual(summary['first_pass_transport_interrupted_video_count'], 1)
        self.assertEqual(summary['transport_interruption_event_count'], 5)
        self.assertEqual(report['status'], 'finished')

    async def test_corrupt_reused_file_stops_before_any_download_or_deletion(self):
        plan, report = self.seed()
        path = self.root / 'videos' / filename(self.records[101])
        bad = bytearray(path.read_bytes())
        bad[-1] ^= 1
        path.write_bytes(bad)
        with self.assertRaisesRegex(StopArchive, 'resume_file_not_verified'):
            audit.resume_existing(self.root, report, self.store)
        self.assertEqual(path.read_bytes(), bytes(bad))
        self.assertEqual(report['dramas']['30']['status'], 'metadata_failed')

    async def test_metadata_disconnect_stops_without_skipping_the_next_drama(self):
        plan, report = self.seed()
        audit.resume_existing(self.root, report, self.store)
        # Exercise a fresh metadata operation first; no real Telegram calls.
        plan['dramas'] = [plan['dramas'][2], plan['dramas'][1]]
        client = self.client()
        with patch.object(audit, 'collect_replies', new=AsyncMock(side_effect=ConnectionError())) as collect:
            with self.assertRaisesRegex(StopArchive, 'connection_recovery_exhausted_during_metadata'):
                await self.run_audit(client, plan, report)
        collect.assert_awaited_once()
        self.assertEqual(report['dramas']['30']['status'], 'queued')
        self.assertEqual(report['dramas']['20']['status'], 'metadata_ready')
        self.assertEqual(report['summary']['unassessed_drama_count'], 0)

    async def test_download_disconnect_does_not_finalize_the_drama_or_burn_five_retries(self):
        plan, report = self.seed(include_metadata=False)
        audit.resume_existing(self.root, report, self.store)
        client = self.client()

        async def failure(client, peer, source, store, record, state, **kwargs):
            state.update(status='failed', attempts=state['attempts'] + 1, failure_reason='ConnectionError')
            state['events'].append({'event': 'attempt_failed', 'reason': 'ConnectionError'})
            store.update(record['message_id'], state)
            return 'failed'

        with patch.object(audit, 'download_one', new=AsyncMock(side_effect=failure)) as download:
            with self.assertRaisesRegex(StopArchive, 'connection_recovery_exhausted_during_download'):
                await self.run_audit(client, plan, report)
        download.assert_awaited_once()
        self.assertEqual(report['dramas']['20']['status'], 'downloading')
        self.assertEqual(report['summary']['finished_drama_count'], 1)
        self.assertEqual(report['summary']['final_failed_video_count'], 0)
        state = next(s for r, s in self.store.videos() if r['message_id'] == 202)
        self.assertEqual((state['attempts'], state['status']), (6, 'pending'))

    async def test_real_video_failure_still_has_a_finite_effective_attempt_budget(self):
        plan, report = self.seed(include_metadata=False)
        audit.resume_existing(self.root, report, self.store)
        client = self.client()

        async def failure(client, peer, source, store, record, state, **kwargs):
            reason = 'message_deleted_or_inaccessible_since_scan'
            state.update(status='failed', attempts=state['attempts'] + 1, failure_reason=reason)
            state['events'].append({'event': 'attempt_failed', 'reason': reason})
            store.update(record['message_id'], state)
            return 'failed'

        with (patch.object(audit, 'download_one', new=AsyncMock(side_effect=failure)) as download,
              patch.object(audit.asyncio, 'sleep', new=AsyncMock())):
            await self.run_audit(client, plan, report)
        self.assertEqual(download.await_count, 5)
        state = next(s for r, s in self.store.videos() if r['message_id'] == 202)
        entry = next(v for v in report['dramas']['20']['videos'] if v['message_id'] == 202)
        self.assertEqual(state['attempts'], 10)
        self.assertEqual(audit.effective_attempts(entry, state), 5)
        self.assertEqual(report['dramas']['20']['delivery_decision'], 'rejected_incomplete')
        self.assertEqual(report['summary']['final_failed_video_count'], 1)

    async def test_remaining_metadata_errors_cannot_be_reported_as_finished(self):
        plan, report = self.seed()
        audit.resume_existing(self.root, report, self.store)
        plan['dramas'] = [plan['dramas'][2]]
        with patch.object(audit, 'collect_replies', new=AsyncMock(side_effect=ValueError('test_metadata_failure'))):
            await self.run_audit(self.client(), plan, report)
        self.assertEqual(report['status'], 'incomplete')
        self.assertEqual(report['summary']['unassessed_drama_count'], 1)

    def test_reconnect_opt_in_does_not_change_other_entry_points(self):
        with patch.object(archive, 'session_paths', return_value=(self.root / 'user', self.root / 'lock')), \
                patch.object(archive, 'TelegramClient') as factory:
            config = {'api_id': 123, 'api_hash': 'test'}
            archive.client_for(config)
            self.assertFalse(factory.call_args.kwargs['auto_reconnect'])
            archive.client_for(config, auto_reconnect=True)
            self.assertTrue(factory.call_args.kwargs['auto_reconnect'])
            self.assertEqual(factory.call_args.kwargs['connection_retries'], 2)


if __name__ == '__main__':
    unittest.main()
