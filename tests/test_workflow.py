"""Workflow sequencing and recovery using only local stores and fake receipts."""
import copy
import sqlite3
import sys
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from video_archive.core import Store
from video_archive.pipeline import runner, workflow
from video_archive.pipeline.common import PipelineError, cover_object_path, identity, object_path
from video_archive.telegram import archive


PROJECT = 'a' * 20


def jobs(count=2, *, topic=20, start=1):
    source = -100123
    did = identity('topic', source, topic)
    result = []
    for mid in range(start, start + count):
        vid = identity('message', source, mid)
        drama = {'id': did, 'source_id': str(source), 'topic_id': str(topic)}
        video = {'id': vid, 'drama_id': did, 'source_id': str(source),
                 'message_id': str(mid), 'media_id': str(9000 + mid)}
        record = {'source': {'id': source}, 'message_id': mid, 'media_id': 9000 + mid,
                  'topic_id': topic, 'expected_size': 100}
        result.append((drama, video, record, {'status': 'pending'}))
    return result


class FakeDB:
    def __init__(self, rows, *, ai=False, transcode='succeeded', missing=()):
        self.states = {}
        self.calls = []
        self.missing = set(missing)
        self.ai_source = rows[0][1]['id'] if rows else None
        for _, video, record, _ in rows:
            state = {**video, 'object_path': object_path(video['id']),
                     'cover_path': cover_object_path(video['id']),
                     'tasks': {'upload': {'status': 'succeeded'},
                               'cover_download': {'status': 'succeeded'},
                               'cover_upload': {'status': 'succeeded'},
                               'ai': {'status': 'pending'}}, 'ai': None}
            if transcode is not None:
                state['tasks']['transcode'] = {'status': transcode}
            self.states[video['id']] = state
        if ai:
            self.commit_ai()

    def archived_messages(self):
        return {int(state['message_id']) for state in self.states.values()
                if int(state['message_id']) not in self.missing}

    def state(self, vid):
        return copy.deepcopy(self.states.get(vid))

    def commit_ai(self):
        for state in self.states.values():
            state['tasks']['ai'] = {'status': 'succeeded'}
            state['ai'] = {'drama_id': state['drama_id'], 'source_video_id': self.ai_source,
                           'title': '测试剧', 'tags': [], 'description': None}

    def claim(self, did, stage, seconds=120):
        self.calls.append(('claim', did, stage))
        return 'fake-token'

    def call(self, name, *args):
        self.calls.append((name, *args))
        if name == 'ai_source_video':
            return self.ai_source
        if name in ('fail', 'renew'):
            return True
        raise AssertionError('Unexpected database call: ' + name)


@contextmanager
def processed_pool(**attributes):
    module = types.ModuleType('video_archive.pipeline.processed')
    for key, value in attributes.items():
        setattr(module, key, value)
    with patch.dict(sys.modules, {'video_archive.pipeline.processed': module}):
        yield


class SourceWorkflowTests(unittest.TestCase):
    def test_scan_uses_metadata_command_inside_lock_and_closes_store_without_touching_argv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            events, stores = [], []

            @contextmanager
            def lock():
                events.append('lock')
                try:
                    yield
                finally:
                    events.append('unlock')

            def store(path):
                current = Store(path)
                stores.append(current)
                return current

            async def execute(args, current):
                events.append('scan')
                self.assertEqual(args.command, 'scan')
                self.assertEqual(args.data, root)
                self.assertEqual(current.root, root.resolve())
                return 0

            def export(current):
                events.append('export')

            argv = ['pipeline.py', 'run', '--not-an-archive-flag']
            with (patch.object(sys, 'argv', argv), patch.object(workflow, 'source_session_lock', lock),
                  patch.object(workflow, 'Store', side_effect=store),
                  patch.object(Store, 'export', export),
                  patch.object(archive, 'execute', new=AsyncMock(side_effect=execute))):
                workflow.scan_source(root)
                self.assertIs(sys.argv, argv)
            self.assertEqual(events, ['lock', 'scan', 'export', 'unlock'])
            with self.assertRaises(sqlite3.ProgrammingError):
                stores[0].db.execute('SELECT 1')

    def test_scan_reuses_an_outer_lock_and_failure_does_not_continue(self):
        with tempfile.TemporaryDirectory() as tmp:
            with (patch.object(workflow, 'source_session_lock', side_effect=AssertionError('nested lock')),
                  patch.object(archive, 'execute', new=AsyncMock(return_value=2))):
                with self.assertRaisesRegex(PipelineError, 'metadata_scan_incomplete'):
                    workflow.scan_source(Path(tmp), lock_held=True)

    def test_only_requests_already_in_pool_do_not_fall_back_to_another_drama(self):
        a, b = jobs(), jobs(topic=30, start=101)
        filter_rows = Mock(return_value=([], [{'drama_id': a[0][1]['drama_id']}]))
        with processed_pool(filter_unprocessed_rows=filter_rows):
            remaining, skipped, only, first = workflow.unprocessed_selection(
                Path('pool.json'), PROJECT, a + b, only=[1], first=[2])
        self.assertEqual((remaining, only, first), ([], [], []))
        self.assertEqual(len(skipped), 1)
        self.assertEqual(filter_rows.call_args.args[2], a)


class AiWorkflowTests(unittest.TestCase):
    def test_already_committed_ai_skips_provider_and_model_call(self):
        rows = jobs(); db = FakeDB(rows, ai=True)
        analyze = Mock()
        with patch('video_archive.pipeline.ai.build_provider') as provider:
            report = workflow.process_ai(db, None, None, rows, analyze=analyze)
        self.assertEqual(report['status'], 'complete')
        self.assertEqual(report['results'][0]['result'], 'ai_already_succeeded')
        analyze.assert_not_called(); provider.assert_not_called()

    def test_ai_runs_after_source_commit_check_and_receipt_is_rechecked(self):
        rows = jobs(); db = FakeDB(rows)
        provider = object()

        def analyze(actual_db, storage, cache, did, actual_provider, *, allowed_source_ids):
            self.assertIs(actual_db, db)
            self.assertIs(actual_provider, provider)
            self.assertEqual(did, rows[0][1]['drama_id'])
            self.assertEqual(allowed_source_ids, {row[1]['id'] for row in rows})
            db.commit_ai()

        report = workflow.process_ai(db, None, None, rows, analyze=analyze, provider=provider)
        self.assertEqual(report['results'][0]['result'], 'ai_succeeded')
        self.assertEqual(report['status'], 'complete')

    def test_commit_response_loss_is_reconciled_without_another_ai_call(self):
        rows = jobs(); db = FakeDB(rows)

        def lost_response(*args, **kwargs):
            db.commit_ai()
            raise OSError('response lost after commit')

        analyze = Mock(side_effect=lost_response)
        first = workflow.process_ai(db, None, None, rows, analyze=analyze, provider=object())
        second = workflow.process_ai(db, None, None, rows, analyze=analyze, provider=object())
        self.assertEqual(first['results'][0]['result'], 'ai_already_succeeded')
        self.assertEqual(second['results'][0]['result'], 'ai_already_succeeded')
        self.assertEqual(analyze.call_count, 1)

    def test_nonmanifest_ai_source_is_rejected_before_provider_access(self):
        rows = jobs(); db = FakeDB(rows)
        db.ai_source = identity('message', -100123, 999)
        provider = Mock(is_test_double=False)
        report = workflow.process_ai(db, None, None, rows, analyze=runner.run_ai, provider=provider)
        self.assertEqual(report['status'], 'pending')
        self.assertEqual(report['results'][0]['error'], 'ai_source_outside_approved_manifest')
        provider.analyze.assert_not_called()

    def test_task_not_claimable_is_pending_and_task_success_without_result_is_not_success(self):
        rows = jobs(); db = FakeDB(rows)
        for state in db.states.values():
            state['tasks']['ai'] = {'status': 'succeeded'}
        analyze = Mock(side_effect=PipelineError('task_not_claimable'))
        report = workflow.process_ai(db, None, None, rows, analyze=analyze, provider=object())
        self.assertEqual(report['status'], 'pending')
        self.assertEqual(report['results'][0]['result'], 'pending')
        analyze.assert_called_once()


class SealWorkflowTests(unittest.TestCase):
    def finalize(self, db, rows):
        record = Mock()
        with processed_pool(record_success=record):
            result = workflow.finalize_processed(db, rows, path=Path('pool.json'), project_ref=PROJECT)
        return result, record

    def test_all_committed_stages_seal_the_whole_drama(self):
        rows = jobs(); result, record = self.finalize(FakeDB(rows, ai=True), rows)
        self.assertEqual(result[0]['result'], 'completed')
        record.assert_called_once()
        self.assertEqual(record.call_args.args[2], rows)
        self.assertEqual(record.call_args.kwargs['details']['missing_video_count'], 0)

    def test_absent_transcode_task_is_not_applicable(self):
        rows = jobs(); result, record = self.finalize(FakeDB(rows, ai=True, transcode=None), rows)
        self.assertEqual(result[0]['result'], 'completed')
        record.assert_called_once()

    def test_approved_missing_members_still_seal_full_source_identity(self):
        rows = jobs(50); db = FakeDB(rows, ai=True, missing={49, 50})
        result, record = self.finalize(db, rows)
        self.assertEqual(result[0]['result'], 'completed')
        self.assertEqual(record.call_args.args[2], rows)
        self.assertEqual(record.call_args.kwargs['details']['missing_video_count'], 2)

    def test_pending_ai_cover_or_applicable_transcode_never_seals(self):
        rows = jobs()
        for stage, reason in (('ai', 'ai_pending'), ('cover_upload', 'cover_pending'),
                              ('transcode', 'transcode_pending')):
            with self.subTest(stage=stage):
                db = FakeDB(rows, ai=True)
                db.states[rows[0][1]['id']]['tasks'][stage]['status'] = 'failed'
                result, record = self.finalize(db, rows)
                self.assertEqual(result[0]['result'], 'pending')
                self.assertEqual(result[0]['reason'], reason)
                record.assert_not_called()


if __name__ == '__main__':
    unittest.main()
