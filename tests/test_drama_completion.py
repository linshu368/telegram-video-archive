"""Exact source membership, persistent failures and repair without real services."""
import asyncio
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.core import atomic_json
from video_archive.pipeline.batch import plan_dramas, process_drama_pass, retry_plan_dramas
from video_archive.pipeline.common import PipelineError
from video_archive.pipeline.completion import manifest_for_jobs, manifest_map, read_pass_report
from test_drama_scheduling import COVER_SUCCESS, POLICY, PROJECT, SUCCESS, drama, mids, segment


def receipt(batch, *, failures=(), project_ref=PROJECT):
    part = segment(batch, failures=failures)
    part.update(project_ref=project_ref, expected_manifest=manifest_for_jobs(batch))
    identities = manifest_map(part['expected_manifest'])
    for entry in part['videos']:
        entry.update(identities[entry['message_id']])
        if entry['result'] == 'failed':
            entry['error'] = 'download_timeout'
    return part


class CompletionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.rows = drama('drama-a', range(101, 151))

    def tearDown(self):
        self.tmp.cleanup()

    async def execute(self, *, rows=None, handler=None, archived=(), covered=(),
                      name='pass', retry_rounds=0, **plan_options):
        rows = self.rows if rows is None else rows
        jobs, deferred, evaluations = plan_dramas(
            rows, self.root, set(archived), covered=set(covered), quality_policy=POLICY, **plan_options)
        path, calls = self.root / (name + '.json'), []
        async def process(*args, **options):
            batch = args[7]
            calls.append(mids(batch))
            return handler(batch, options, len(calls)) if handler else receipt(batch)
        with patch('video_archive.pipeline.batch.process_batch', process), \
                patch('video_archive.pipeline.batch.asyncio.sleep', new=AsyncMock()):
            report = await process_drama_pass(
                *([None] * 7), jobs, drama_evaluations=evaluations, deferred=deferred,
                report_path=path, project_ref=PROJECT, retry_rounds=retry_rounds)
        self.assertEqual(json.loads(path.read_text()), report)
        return report, calls

    async def test_50_members_missing_two_then_repair_only_two_produces_exact_50(self):
        failed = {123, 149}
        first, calls = await self.execute(handler=lambda batch, *_: receipt(batch, failures=failed))
        evaluation = first['dramas'][0]
        self.assertEqual((evaluation['expected_count'], evaluation['verified_video_count'],
                          evaluation['missing_video_count']), (50, 48, 2))
        self.assertEqual(evaluation['remaining_video_message_ids'], [123, 149])
        self.assertEqual({row['message_id'] for row in evaluation['failure_history']}, failed)
        self.assertEqual(len(evaluation['episodes']), 50)
        self.assertEqual(len(evaluation['expected_manifest']), 50)
        committed = set(range(101, 151)) - failed
        repaired, calls = await self.execute(
            name='repair', archived=committed, covered=committed,
            resume_partials=True, defer_ids=(), force_readback=False)
        self.assertEqual(calls, [[123, 149]])
        completed = repaired['dramas'][0]
        self.assertEqual((completed['expected_count'], completed['verified_video_count'],
                          completed['missing_video_count']), (50, 50, 0))
        self.assertEqual(set(completed['verified_message_ids']), set(range(101, 151)))
        self.assertEqual(completed['state'], 'complete')
        # A new repair report never overwrites the previous failure evidence.
        self.assertEqual(json.loads((self.root / 'pass.json').read_text())['dramas'][0]['missing_video_count'], 2)

    async def test_bounded_retry_keeps_failed_attempts_after_all_50_succeed(self):
        report, calls = await self.execute(retry_rounds=1, handler=lambda batch, _, attempt:
                                           receipt(batch, failures={123, 149} if attempt == 1 else ()))
        evaluation = report['dramas'][0]
        self.assertEqual([len(batch) for batch in calls], [50, 2])
        self.assertEqual(evaluation['verified_video_count'], 50)
        self.assertEqual(evaluation['missing_video_count'], 0)
        failed_episode = next(row for row in evaluation['episodes'] if row['message_id'] == 123)
        self.assertEqual(len(failed_episode['attempt_history']), 2)
        self.assertEqual(failed_episode['failure_history'][0]['error'], 'download_timeout')
        self.assertEqual(failed_episode['video_status'], 'verified')
        self.assertEqual(evaluation['completion_identity_status'], 'matched_manifest')

    async def test_50_receipts_with_one_wrong_message_cannot_complete(self):
        def wrong(batch, *_):
            part = receipt(batch); part['videos'][-1]['message_id'] = 999
            return part
        with self.assertRaisesRegex(PipelineError, 'unexpected_id'):
            await self.execute(handler=wrong)
        report = json.loads((self.root / 'pass.json').read_text())
        self.assertEqual(report['status'], 'interrupted')
        self.assertFalse(report['dramas'][0]['video_complete'])

    async def test_duplicate_receipts_cannot_replace_a_missing_member(self):
        def duplicate(batch, *_):
            part = receipt(batch); part['videos'][-1] = dict(part['videos'][-2])
            return part
        with self.assertRaisesRegex(PipelineError, 'duplicate_id'):
            await self.execute(handler=duplicate)

    async def test_explicit_wrong_media_or_source_identity_is_rejected(self):
        for field, value in (('media_id', '999999'), ('source_id', '-100999')):
            with self.subTest(field=field):
                def wrong(batch, *_):
                    part = receipt(batch); part['videos'][-1][field] = value
                    return part
                with self.assertRaisesRegex(PipelineError, 'identity_conflict'):
                    await self.execute(handler=wrong)

    async def test_duplicate_expected_member_is_rejected_before_transfer(self):
        jobs, _, evaluations = plan_dramas(self.rows, self.root, set(), quality_policy=POLICY)
        evaluations[0]['expected_message_ids'][-1] = evaluations[0]['expected_message_ids'][0]
        with patch('video_archive.pipeline.batch.process_batch', new=AsyncMock()) as transfer:
            with self.assertRaisesRegex(PipelineError, 'duplicate_identity'):
                await process_drama_pass(*([None] * 7), jobs, drama_evaluations=evaluations)
        transfer.assert_not_called()

    async def test_interrupted_child_receipts_are_merged_with_error_details(self):
        def interrupt(batch, options, _):
            part = receipt(batch, failures={123, 149})
            part.update(status='interrupted')
            atomic_json(options['report_path'], part)
            raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.execute(handler=interrupt)
        report = json.loads((self.root / 'pass.json').read_text())
        self.assertEqual(report['active_segment_recovery'], 'recovered')
        self.assertEqual(len(report['segments']), 1)
        self.assertTrue(report['segments'][0]['recovered_after_interruption'])
        evaluation = report['dramas'][0]
        self.assertEqual(evaluation['verified_video_count'], 48)
        self.assertEqual(evaluation['missing_video_count'], 2)
        self.assertEqual(report['pending_message_ids'], [123, 149])
        self.assertEqual(evaluation['failure_history'][0]['error'], 'download_timeout')

    async def test_retry_reads_valid_active_child_even_if_parent_did_not_append_it(self):
        _, _, evaluations = plan_dramas(self.rows, self.root, set(), quality_policy=POLICY)
        path = self.root / 'interrupted.json'
        child = self.root / 'interrupted-segment-1.json'
        atomic_json(child, receipt(self.rows, failures={123, 149}))
        atomic_json(path, {'project_ref': PROJECT, 'status': 'interrupted', 'segments': [],
                          'dramas': evaluations, 'active_segment_report': str(child),
                          'active_segment_message_ids': list(range(101, 151)),
                          'pending_message_ids': [123, 149]})
        committed = set(range(101, 151)) - {123, 149}
        selected, _, evaluations = retry_plan_dramas(
            self.rows, self.root, path, PROJECT, quality_policy=POLICY,
            archived=committed, covered=committed)
        self.assertEqual(mids(selected), [123, 149])
        self.assertEqual(evaluations[0]['retry_source_identity'], 'matched_source_manifest')
        recovered = read_pass_report(path, PROJECT)
        self.assertEqual(recovered['segments'][0]['videos'][22]['error'], 'download_timeout')
        self.assertEqual(len(recovered['segments']), 1)

    async def test_active_child_with_other_project_or_nonmember_is_rejected(self):
        _, _, evaluations = plan_dramas(self.rows, self.root, set(), quality_policy=POLICY)
        path, child = self.root / 'old.json', self.root / 'old-segment-1.json'
        parent = {'project_ref': PROJECT, 'status': 'interrupted', 'segments': [],
                  'dramas': evaluations, 'active_segment_report': str(child)}
        atomic_json(path, parent)
        atomic_json(child, receipt(self.rows, project_ref='b' * 20))
        with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
            read_pass_report(path, PROJECT)
        part = receipt(self.rows); part['videos'][0]['message_id'] = 999
        atomic_json(child, part)
        with self.assertRaisesRegex(PipelineError, 'unexpected_id'):
            read_pass_report(path, PROJECT)

    async def test_retry_manifest_rejects_source_media_size_and_member_changes(self):
        report, _ = await self.execute(handler=lambda batch, *_: receipt(batch, failures={123}))
        for change in ('source', 'media', 'size', 'member'):
            with self.subTest(change=change):
                rows = copy.deepcopy(self.rows)
                if change == 'source':
                    rows[0][2]['source']['id'] = -100999
                elif change == 'media':
                    rows[0][2]['media_id'] = 999999
                elif change == 'size':
                    rows[0][2]['expected_size'] += 1
                else:
                    rows.pop()
                with self.assertRaisesRegex(PipelineError, 'source_manifest_conflict'):
                    retry_plan_dramas(rows, self.root, self.root / 'pass.json', PROJECT,
                                      quality_policy=POLICY)

    async def test_legacy_report_and_results_are_labeled_without_fabricating_identity(self):
        path = self.root / 'old.json'
        atomic_json(path, {'project_ref': PROJECT, 'videos': [
            {'message_id': 123, 'result': 'failed', 'cover_result': COVER_SUCCESS}]})
        _, _, evaluations = retry_plan_dramas(self.rows, self.root, path, PROJECT, quality_policy=POLICY)
        self.assertEqual(evaluations[0]['retry_source_identity'], 'legacy_report_manifest_missing')
        def legacy(batch, *_):
            part = segment(batch)
            for entry in part['videos']:
                for field in ('source_id', 'media_id', 'expected_size', 'drama_id', 'topic_id'):
                    entry.pop(field, None)
            return part
        report, _ = await self.execute(handler=legacy)
        evaluation = report['dramas'][0]
        self.assertEqual(evaluation['state'], 'complete')
        self.assertEqual(evaluation['completion_identity_status'], 'legacy_result_identity_missing')
        self.assertEqual(evaluation['delivery_decision'], 'pending')
        self.assertIn('media_id', evaluation['episodes'][0]['attempt_history'][0]['missing_identity_fields'])
        self.assertNotIn('media_id', report['segments'][0]['videos'][0])


if __name__ == '__main__':
    unittest.main()
