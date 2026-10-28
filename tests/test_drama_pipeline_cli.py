"""Source-inventory gates and drama selection wiring; no external services."""
import asyncio
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.core import Store, record_message
from video_archive.pipeline.runner import download_quality_policy, main
from video_archive.pipeline.common import PipelineError, identity, snapshot
from video_archive.screening.quality import QualityPolicy
from test_archive import SOURCE, message


def source_inventory(root, *, complete=True, source_complete=True):
    source = {**SOURCE, 'forum': True}
    store = Store(root)
    store.start(source, '2025-01-02T00:00:00+00:00', 100)
    with store.db:
        store.set('cutoff_exclusive_utc', '2025-01-02T00:00:00+00:00')
    records = []
    for mid in (11, 12):
        record = record_message(message(mid), source)
        record.update(topic_id=10, width=720, height=1280, expected_size=8_000_000,
                      duration_seconds=20)
        records.append(record)
    store.ingest(records, 101, 11, [12, 11])
    if complete:
        store.ingest([], 11, 11, [])
    boundary = {'source_id': source['id'], 'upper_id': 100,
                'cutoff_exclusive_utc': '2025-01-02T00:00:00+00:00'}
    proof = {**boundary, 'topic_id': 10, 'topic_title': 'source drama',
             'complete': True, 'reached_empty_page': True,
             'video_metadata': [{key: r[key] for key in (
                 'message_id', 'media_id', 'width', 'height', 'expected_size', 'duration_seconds')}
                                for r in records]}
    store.save_topic_inventory(proof)
    with store.db:
        store.set('source_topic_inventory_complete', source_complete)
        store.set('source_topic_ids', [10])
        store.set('source_topic_inventory_boundary', boundary)
    store.db.close()
    return records, proof


class SourceSnapshotTests(unittest.TestCase):
    def test_global_history_alone_is_not_a_complete_drama(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_inventory(tmp, source_complete=False)
            with self.assertRaisesRegex(PipelineError, 'source_topic_inventory_required'):
                snapshot(tmp, Path(tmp) / 'unused.json', require_complete=True)

    def test_partial_global_scan_is_rejected_even_with_topic_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_inventory(tmp, complete=False)
            with self.assertRaisesRegex(PipelineError, 'inventory_scan_incomplete'):
                snapshot(tmp, Path(tmp) / 'unused.json', require_complete=True)

    def test_source_proof_members_not_local_pool_define_the_drama(self):
        with tempfile.TemporaryDirectory() as tmp:
            records, _ = source_inventory(tmp)
            store = Store(tmp)
            extra = {**records[0], 'message_id': 13, 'media_id': 9013}
            store.ingest_topic_records([extra])
            store.db.close()
            rows, excluded = snapshot(tmp, Path(tmp) / 'not_required.json', require_complete=True)
            self.assertEqual([r[2]['message_id'] for r in rows], [11, 12])
            self.assertEqual(excluded, {'outside_source_topic_inventory': 1})
            self.assertEqual(rows[0][1]['bitrate_bps'], 3_200_000)
            self.assertEqual(rows[0][0]['title'], 'source drama')

    def test_source_member_missing_from_local_metadata_never_shrinks_drama(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, proof = source_inventory(tmp)
            store = Store(tmp)
            proof['video_metadata'].append({**proof['video_metadata'][0],
                                            'message_id': 13, 'media_id': 9013})
            store.save_topic_inventory(proof)
            store.db.close()
            with self.assertRaisesRegex(PipelineError, 'source_topic_inventory_records_missing'):
                snapshot(tmp, Path(tmp) / 'unused.json', require_complete=True)

    def test_fresh_topic_dimensions_override_older_pool_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, proof = source_inventory(tmp)
            proof['video_metadata'][1].update(width=1280, height=720)
            store = Store(tmp); store.save_topic_inventory(proof); store.db.close()
            rows, _ = snapshot(tmp, Path(tmp) / 'unused.json', require_complete=True)
            self.assertEqual((rows[1][1]['width'], rows[1][1]['height']), (1280, 720))

    def test_source_proof_boundary_cannot_be_reused_for_another_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, proof = source_inventory(tmp)
            proof['upper_id'] = 99
            store = Store(tmp); store.save_topic_inventory(proof); store.db.close()
            with self.assertRaisesRegex(PipelineError, 'source_topic_inventory_incomplete'):
                snapshot(tmp, Path(tmp) / 'unused.json', require_complete=True)


class DramaCliTests(unittest.TestCase):
    def call_plan(self, root, *extra):
        args = ['pipeline.py', '--data', str(root), 'plan', *extra]
        output = io.StringIO()
        with patch.object(sys, 'argv', args), contextlib.redirect_stdout(output):
            result = main()
        return result, json.loads(output.getvalue()) if result == 0 else output.getvalue()

    def test_plan_is_offline_and_reports_complete_member_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root)
            report = root / 'selection.json'
            with patch('video_archive.pipeline.runner.Database') as database, patch('video_archive.pipeline.runner.Storage') as storage:
                code, summary = self.call_plan(root, '--report', str(report))
            self.assertEqual(code, 0)
            database.assert_not_called(); storage.assert_not_called()
            self.assertEqual(summary['eligible_before_cloud_check'], 2)
            decision = json.loads(report.read_text())['dramas'][0]
            self.assertEqual(decision['expected_message_ids'], [11, 12])
            self.assertEqual(decision['decision'], 'accept')

    def test_only_does_not_hide_a_landscape_sibling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); _, proof = source_inventory(root)
            proof['video_metadata'][1].update(width=1280, height=720)
            store = Store(root); store.save_topic_inventory(proof); store.db.close()
            code, report = self.call_plan(root, '--only', '11')
            self.assertEqual(code, 0)
            self.assertEqual(report['eligible_before_cloud_check'], 0)
            self.assertEqual(report['quality_decisions'], {'reject': 1})

    def test_only_selects_every_member_of_a_qualified_drama(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root)
            path = root / 'selected.json'
            code, report = self.call_plan(root, '--only', '11', '--report', str(path))
            self.assertEqual(code, 0)
            self.assertEqual(report['eligible_before_cloud_check'], 2)
            decision = json.loads(path.read_text())['dramas'][0]
            self.assertEqual(decision['expected_message_ids'], [11, 12])
            self.assertEqual(decision['scheduled_message_ids'], [11, 12])

    def test_drama_budget_is_available_for_offline_planning(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root)
            code, report = self.call_plan(root, '--drama-limit', '1')
            self.assertEqual(code, 0)
            self.assertEqual(report['eligible_before_cloud_check'], 2)
            self.assertEqual(report['planning_decisions'], {'scheduled': 1})

    def test_video_target_does_not_reject_or_truncate_the_current_drama(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root)
            code, report = self.call_plan(root, '--limit', '1')
            self.assertEqual(code, 0)
            self.assertEqual(report['accepted_drama_videos'], 2)
            self.assertEqual(report['eligible_before_cloud_check'], 2)
            self.assertEqual(report['deferred_count'], 0)

    def test_incomplete_source_stops_before_configuration_or_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root, source_complete=False)
            with patch('video_archive.pipeline.runner.config') as config, patch('video_archive.pipeline.runner.Database') as db:
                code, _ = self.call_plan(root)
            self.assertEqual(code, 2)
            config.assert_not_called(); db.assert_not_called()

    def test_retry_preserves_saved_quality_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / 'report.json'
            report.write_text(json.dumps({'quality_policy': QualityPolicy(minimum_grade=1).to_dict()}))
            args = SimpleNamespace(command='retry', minimum_quality=None, report=report)
            self.assertEqual(download_quality_policy(args).minimum_grade, 1)
            args.minimum_quality = 1080
            self.assertEqual(download_quality_policy(args).minimum_grade, 3)

    def test_retry_cannot_bypass_rejected_drama_gate(self):
        from video_archive.pipeline.batch import retry_plan_dramas
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); _, proof = source_inventory(root)
            proof['video_metadata'][1].update(width=1280, height=720)
            store = Store(root); store.save_topic_inventory(proof); store.db.close()
            rows, _ = snapshot(root, root / 'unused.json', require_complete=True)
            report = root / 'retry.json'
            report.write_text(json.dumps({'project_ref': 'testref', 'videos': [
                {'message_id': 11, 'result': 'failed', 'cover_result': 'failed'}]}))
            jobs, deferred, evaluations = retry_plan_dramas(
                rows, root, report, 'testref', quality_policy=QualityPolicy())
            self.assertEqual((jobs, deferred), ([], []))
            self.assertEqual(evaluations[0]['decision'], 'reject')

    def test_run_filters_before_handing_jobs_to_existing_downloader(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); _, proof = source_inventory(root)
            proof['video_metadata'][1].update(width=1280, height=720)
            store = Store(root); store.save_topic_inventory(proof); store.db.close()
            db, storage = MagicMock(), MagicMock()
            db.archived_messages.return_value = set()
            storage.limit = 128 * 1024**2
            config = {'database_dsn': 'unused', 'project_ref': 'testref',
                      'global_file_limit': 128 * 1024**2, 'bucket_file_limit': 128 * 1024**2}
            runner = AsyncMock(return_value={'status': 'all_archived', 'videos': []})
            args = ['pipeline.py', '--data', str(root), 'run', '--only', '11']
            with (patch.object(sys, 'argv', args), patch('video_archive.pipeline.runner.BASE', root),
                  patch('video_archive.pipeline.runner.config', return_value=config),
                  patch('video_archive.pipeline.runner.Database', return_value=db),
                  patch('video_archive.pipeline.runner.Storage', return_value=storage), patch('video_archive.pipeline.runner.Cache'),
                  patch('video_archive.pipeline.runner.backup_legacy'),
                  patch('video_archive.pipeline.batch.run_batch', runner),
                  patch('video_archive.pipeline.batch.drain_transcode', new=AsyncMock(return_value={'counts': {}})),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(main(), 0)
            self.assertEqual(runner.call_args.args[5], [])
            selection = json.loads(next(root.glob('pipeline-selection-*.json')).read_text())
            self.assertEqual(selection['dramas'][0]['expected_message_ids'], [11, 12])
            self.assertEqual(selection['dramas'][0]['decision'], 'reject')

    def test_batch_accepts_one_drama_without_a_video_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source_inventory(root)
            db, storage = MagicMock(), MagicMock()
            db.archived_messages.return_value = set()
            storage.limit = 128 * 1024**2
            config = {'database_dsn': 'unused', 'project_ref': 'testref',
                      'global_file_limit': 128 * 1024**2, 'bucket_file_limit': 128 * 1024**2}
            runner = AsyncMock(return_value={'status': 'all_archived', 'videos': []})
            args = ['pipeline.py', '--data', str(root), 'batch', '--drama-limit', '1', '--batch-size', '500']
            with (patch.object(sys, 'argv', args), patch('video_archive.pipeline.runner.BASE', root),
                  patch('video_archive.pipeline.runner.config', return_value=config),
                  patch('video_archive.pipeline.runner.Database', return_value=db),
                  patch('video_archive.pipeline.runner.Storage', return_value=storage),
                  patch('video_archive.pipeline.runner.Cache'),
                  patch('video_archive.pipeline.runner.backup_legacy'),
                  patch('video_archive.pipeline.batch.run_batch', runner),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(main(), 0)
            self.assertEqual([job[2]['message_id'] for job in runner.call_args.args[5]], [11, 12])
            self.assertEqual(runner.call_args.kwargs['batch_size'], 500)
            decision = runner.call_args.kwargs['drama_evaluations'][0]
            self.assertEqual(decision['scheduled_message_ids'], [11, 12])


if __name__ == '__main__':
    unittest.main()
