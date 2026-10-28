"""Source-specific receipts and repair selection, without database or media IO."""
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.core import filename
from video_archive.pipeline.common import PipelineError, cover_object_path, object_path, snapshot
from video_archive.pipeline.runner import committed_inventory, main
from video_archive.pipeline.outcomes import outcome_file, write_outcomes
from video_archive.pipeline.completion import manifest_for_jobs
from test_drama_pipeline_cli import source_inventory
from test_pipeline import fixture


def inventory_job(mid=11, source=-100123):
    drama, video = fixture(mid, source=source)
    record = {'source': {'id': source}, 'message_id': mid, 'media_id': 9000 + mid}
    return drama, video, record, {}


def committed(job, *, cover=True):
    video = job[1]
    return {**video, 'object_path': object_path(video['id']),
            'cover_path': cover_object_path(video['id']) if cover else None,
            'tasks': {stage: {'status': 'succeeded'} for stage in (
                'download', 'upload', 'cover_download', 'cover_upload')}}


class ReceiptTests(unittest.TestCase):
    def test_same_message_number_in_other_source_is_not_a_completed_episode(self):
        row = inventory_job()
        other = inventory_job(source=-100456)
        db = MagicMock()
        db.archived_messages.return_value = {11}
        db.state.side_effect = lambda vid: committed(other) if vid == other[1]['id'] else None
        self.assertEqual(committed_inventory(db, [row]), (set(), set()))
        db.state.assert_called_once_with(row[1]['id'])

    def test_matching_committed_identity_and_cover_are_counted(self):
        row = inventory_job()
        db = MagicMock()
        db.archived_messages.return_value = {11}
        db.state.return_value = committed(row)
        self.assertEqual(committed_inventory(db, [row]), ({11}, {11}))

    def test_wrong_media_identity_cannot_be_used_as_a_receipt(self):
        row = inventory_job()
        db = MagicMock()
        db.archived_messages.return_value = {11}
        db.state.return_value = {**committed(row), 'media_id': '123'}
        with self.assertRaisesRegex(PipelineError, 'archive_identity_conflict'):
            committed_inventory(db, [row])

    def test_object_path_without_successful_commit_is_not_complete(self):
        row = inventory_job()
        cloud = committed(row)
        cloud['tasks']['upload']['status'] = 'failed'
        db = MagicMock()
        db.archived_messages.return_value = {11}
        db.state.return_value = cloud
        self.assertEqual(committed_inventory(db, [row]), (set(), set()))

    def test_cover_completion_comes_from_cloud_stages(self):
        row = inventory_job()
        cloud = committed(row)
        cloud['tasks']['cover_upload']['status'] = 'pending'
        db = MagicMock()
        db.archived_messages.return_value = {11}
        db.state.return_value = cloud
        self.assertEqual(committed_inventory(db, [row]), ({11}, set()))
        cloud['cover_path'] = None
        cloud['cover_note'] = 'no_thumbnail_available'
        cloud['tasks']['cover_upload']['status'] = 'succeeded'
        self.assertEqual(committed_inventory(db, [row]), ({11}, {11}))


class RepairCliTests(unittest.TestCase):
    def test_repair_needs_no_old_report_and_only_schedules_the_missing_member(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records, _ = source_inventory(root)
            rows, _ = snapshot(root, root / 'unused.json', require_complete=True)
            partial = root / 'videos' / (filename(records[1]) + '.part')
            partial.write_bytes(b'keep')
            db, storage = MagicMock(), MagicMock()
            db.archived_messages.return_value = {11}
            db.state.side_effect = lambda vid: committed(rows[0]) if vid == rows[0][1]['id'] else None
            storage.limit = 128 * 1024**2
            config = {'database_dsn': 'unused', 'project_ref': 'testref',
                      'global_file_limit': 128 * 1024**2, 'bucket_file_limit': 128 * 1024**2}
            runner = AsyncMock(return_value={'status': 'all_archived', 'videos': []})
            args = ['pipeline.py', '--data', str(root), 'repair', '--only', '11',
                    '--drama-concurrency', '2']
            output = io.StringIO()
            with (patch.object(sys, 'argv', args), patch('video_archive.pipeline.runner.BASE', root),
                  patch('video_archive.pipeline.runner.config', return_value=config),
                  patch('video_archive.pipeline.runner.Database', return_value=db),
                  patch('video_archive.pipeline.runner.Storage', return_value=storage),
                  patch('video_archive.pipeline.runner.Cache'),
                  patch('video_archive.pipeline.runner.backup_legacy'),
                  patch('video_archive.pipeline.batch.run_batch', runner),
                  contextlib.redirect_stdout(output)):
                self.assertEqual(main(), 0)
            self.assertEqual([job[2]['message_id'] for job in runner.call_args.args[5]], [12])
            self.assertTrue(runner.call_args.kwargs['resume_partials'])
            self.assertEqual(runner.call_args.kwargs['retry_rounds'], 4)
            self.assertEqual(runner.call_args.kwargs['drama_concurrency'], 2)
            self.assertEqual(partial.read_bytes(), b'keep')
            decision = runner.call_args.kwargs['drama_evaluations'][0]
            self.assertEqual(decision['expected_message_ids'], [11, 12])
            self.assertEqual(decision['committed_message_ids'], [11])
            self.assertEqual(json.loads(output.getvalue())['status'], 'all_archived')


class DownstreamGateTests(unittest.TestCase):
    def test_unapproved_drama_never_reaches_ai_or_transcode(self):
        for command in ('ai', 'transcode-backlog'):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); source_inventory(root)
                config = {'database_dsn': 'unused', 'project_ref': 'testref'}
                db, storage = MagicMock(), MagicMock()
                output = io.StringIO()
                args = ['pipeline.py', '--data', str(root), command]
                with (patch.object(sys, 'argv', args), patch('video_archive.pipeline.runner.BASE', root),
                      patch('video_archive.pipeline.runner.config', return_value=config),
                      patch('video_archive.pipeline.runner.Database', return_value=db),
                      patch('video_archive.pipeline.runner.Storage', return_value=storage),
                      patch('video_archive.pipeline.runner.run_ai') as ai,
                      patch('video_archive.pipeline.runner.transcode_task') as transcode,
                      patch('video_archive.pipeline.ai.build_provider') as provider,
                      contextlib.redirect_stdout(output)):
                    self.assertEqual(main(), 0)
                ai.assert_not_called(); transcode.assert_not_called(); provider.assert_not_called()
                db.archived_messages.assert_not_called(); storage.bucket.assert_not_called()
                self.assertEqual(json.loads(output.getvalue())['reason'], 'download_completeness_not_approved')

    def test_finalized_matching_source_can_reach_ai_without_legacy_sample_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root)
            rows, _ = snapshot(root, root / 'unused.json', require_complete=True)
            did = rows[0][1]['drama_id']
            write_outcomes(outcome_file(root, 'testref'), 'testref', [{
                'drama_id': did, 'decision': 'accept', 'delivery_accepted': True,
                'delivery_finalized': True, 'delivery_decision': 'complete',
                'expected_manifest': manifest_for_jobs(rows)}])
            db, storage = MagicMock(), MagicMock()
            db.archived_messages.return_value = {11, 12}
            receipts = {job[1]['id']: committed(job) for job in rows}
            db.state.side_effect = receipts.get
            config = {'database_dsn': 'unused', 'project_ref': 'testref'}
            args = ['pipeline.py', '--data', str(root), 'ai']
            def commit_ai(*args, **kwargs):
                for receipt in receipts.values():
                    receipt['tasks']['ai'] = {'status': 'succeeded'}
                    receipt['ai'] = {'drama_id': did, 'source_video_id': rows[0][1]['id'],
                                     'title': '剧名', 'tags': [], 'description': None}
            with (patch.object(sys, 'argv', args), patch('video_archive.pipeline.runner.BASE', root),
                  patch('video_archive.pipeline.runner.config', return_value=config),
                  patch('video_archive.pipeline.runner.Database', return_value=db),
                  patch('video_archive.pipeline.runner.Storage', return_value=storage),
                  patch('video_archive.pipeline.runner.Cache'),
                  patch('video_archive.pipeline.runner.run_ai', side_effect=commit_ai) as ai,
                  patch('video_archive.pipeline.ai.build_provider'),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(main(), 0)
            self.assertEqual(ai.call_count, 1)
            self.assertEqual(ai.call_args.args[3], did)
