"""Whole-drama planning and finite execution; no Telegram or database calls."""
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.core import filename
from video_archive.pipeline.batch import (plan_dramas, process_drama_pass,
                                          retry_plan_dramas, run_batch)
from video_archive.pipeline.common import PipelineError
from video_archive.screening.quality import QualityPolicy


POLICY = QualityPolicy()
PROJECT = 'a' * 20
SUCCESS = 'uploaded_readback_verified'
COVER_SUCCESS = 'cover_uploaded_readback_verified'


def drama(did, mids):
    return [({'id': did, 'title': did}, {'id': f'video-{mid}', 'drama_id': did},
             {'message_id': mid, 'source': {'id': -100123}, 'media_id': 9000 + mid,
              'original_filename': 'fixture.mp4', 'expected_size': 2_500_000,
              'duration_seconds': 10, 'width': 720, 'height': 1280,
              'bitrate_bps': 2_000_000}, {'status': 'pending'}) for mid in mids]


def mids(jobs):
    return [job[2]['message_id'] for job in jobs]


def segment(jobs, *, failures=(), cover_failures=(), stop=None):
    return {'status': 'stopped' if stop else 'all_archived', 'stop_reason': stop,
            'download_limit_now': 4,
            'videos': [{'message_id': job[2]['message_id'], 'video_id': job[1]['id'],
                        'result': 'failed' if job[2]['message_id'] in failures else SUCCESS,
                        'cover_result': 'failed' if job[2]['message_id'] in cover_failures else COVER_SUCCESS}
                       for job in jobs]}


class DramaPlannerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def plan(self, rows, archived=(), **kwargs):
        return plan_dramas(rows, self.root, set(archived), quality_policy=POLICY, **kwargs)

    def test_only_expands_to_whole_drama_before_quality_filter(self):
        rows = drama('a', [101, 105]) + drama('b', [102])
        selected, _, evaluations = self.plan(rows, only=[101])
        self.assertEqual(mids(selected), [101, 105])
        self.assertEqual(evaluations[0]['expected_message_ids'], [101, 105])
        rows[1][2].update(width=1280, height=720)
        selected, _, evaluations = self.plan(rows, only=[101])
        self.assertEqual(selected, [])
        self.assertEqual(evaluations[0]['planning_status'], 'rejected')

    def test_first_prioritizes_a_whole_drama_even_if_source_messages_interleave(self):
        a, b = drama('a', [101, 105]), drama('b', [102, 103])
        selected, _, _ = self.plan([a[0], b[0], b[1], a[1]], first=[102])
        self.assertEqual(mids(selected), [102, 103, 101, 105])

    def test_video_target_smaller_than_first_drama_still_admits_whole_drama(self):
        selected, deferred, evaluations = self.plan(drama('a', [101, 102, 103]), video_limit=2)
        self.assertEqual(mids(selected), [101, 102, 103])
        self.assertEqual(deferred, [])
        self.assertEqual(evaluations[0]['planning_status'], 'scheduled')

    def test_video_target_allows_current_drama_to_finish_past_remaining_target(self):
        selected, deferred, _ = self.plan(
            drama('a', [101, 102, 103]) + drama('b', [104, 105]), video_limit=4)
        self.assertEqual(mids(selected), [101, 102, 103, 104, 105])
        self.assertEqual(deferred, [])

    def test_reaching_video_target_stops_only_before_next_drama(self):
        selected, deferred, evaluations = self.plan(
            drama('a', [101, 102, 103]) + drama('b', [104, 105]), video_limit=3)
        self.assertEqual(mids(selected), [101, 102, 103])
        self.assertEqual({entry['message_id'] for entry in deferred}, {104, 105})
        self.assertEqual(evaluations[1]['planning_reason'], 'drama_video_target_reached')

    def test_drama_limit_admits_every_member_of_one_drama(self):
        selected, deferred, evaluations = self.plan(
            drama('a', [101, 102]) + drama('b', [103, 104]), drama_limit=1)
        self.assertEqual(mids(selected), [101, 102])
        self.assertEqual(len(deferred), 2)
        self.assertEqual(evaluations[1]['planning_reason'], 'drama_count_limit')

    def test_accepted_low_quality_outlier_is_still_downloaded(self):
        rows = drama('a', range(101, 111))
        rows[-1][2].update(width=480, height=854, bitrate_bps=500_000)
        selected, _, evaluations = self.plan(rows)
        self.assertEqual(len(selected), 10)
        self.assertEqual(evaluations[0]['ignored_low_count'], 1)

    def test_missing_metadata_keeps_whole_drama_pending(self):
        rows = drama('a', [101, 102])
        rows[1][2]['width'] = None
        selected, deferred, evaluations = self.plan(rows)
        self.assertEqual((selected, deferred), ([], []))
        self.assertEqual(evaluations[0]['planning_status'], 'pending')
        self.assertEqual(evaluations[0]['expected_message_ids'], [101, 102])

    def test_one_blocked_or_oversize_member_defers_the_whole_drama(self):
        for kind in ('blocked', 'skipped', 'large', 'bucket', 'explicit'):
            with self.subTest(kind=kind):
                rows, options = drama('a', [101, 102]), {}
                if kind in ('blocked', 'skipped'):
                    rows[1][3]['status'] = kind
                elif kind == 'large':
                    rows[1][2]['expected_size'] = 200 * 1024**2
                elif kind == 'bucket':
                    options['bucket_limit'] = 1_000_000
                else:
                    options['defer_ids'] = [102]
                selected, deferred, evaluations = self.plan(rows, **options)
                self.assertEqual(selected, [])
                self.assertEqual({entry['message_id'] for entry in deferred}, {101, 102})
                self.assertEqual(evaluations[0]['planning_status'], 'deferred')

    def test_partial_member_defers_whole_group_but_retry_can_resume(self):
        rows = drama('a', [101, 102])
        path = self.root / 'videos' / (filename(rows[1][2]) + '.part')
        path.parent.mkdir(); path.write_bytes(b'old partial')
        selected, deferred, _ = self.plan(rows)
        self.assertEqual(selected, [])
        self.assertEqual(len(deferred), 2)
        selected, deferred, _ = self.plan(rows, resume_partials=True)
        self.assertEqual(mids(selected), [101, 102])
        self.assertEqual(deferred, [])
        self.assertEqual(path.read_bytes(), b'old partial')

    def test_remote_complete_members_skip_and_cover_only_bypasses_video_size(self):
        rows = drama('a', [101, 102, 103])
        rows[1][2]['expected_size'] = 900 * 1024**2
        selected, deferred, evaluations = self.plan(rows, archived={101, 102}, covered={101})
        self.assertEqual(mids(selected), [102, 103])
        self.assertEqual(deferred, [])
        self.assertEqual(evaluations[0]['expected_message_ids'], [101, 102, 103])
        self.assertEqual(evaluations[0]['complete_message_ids'], [101])
        self.assertEqual(evaluations[0]['cover_only_message_ids'], [102])

    def test_only_explicit_committed_id_is_reverified_not_completed_siblings(self):
        rows = drama('a', [101, 102, 103])
        selected, _, evaluations = self.plan(rows, archived={101, 102, 103},
                                              covered={101, 102, 103}, only=[102])
        self.assertEqual(mids(selected), [102])
        self.assertEqual(evaluations[0]['cover_only_message_ids'], [])

    def test_retry_expands_failure_to_whole_group_and_rechecks_quality(self):
        rows = drama('a', [101, 102])
        report = self.root / 'retry.json'
        report.write_text(json.dumps({'project_ref': PROJECT, 'videos': [
            {'message_id': 101, 'result': 'failed', 'cover_result': COVER_SUCCESS}]}))
        selected, _, evaluations = retry_plan_dramas(
            rows, self.root, report, PROJECT, quality_policy=POLICY)
        self.assertEqual(mids(selected), [101, 102])
        rows[1][2].update(width=1280, height=720)
        selected, _, evaluations = retry_plan_dramas(
            rows, self.root, report, PROJECT, quality_policy=POLICY)
        self.assertEqual(selected, [])
        self.assertEqual(evaluations[0]['planning_status'], 'rejected')

    def test_retry_uses_live_commits_and_finishes_drama_past_video_target(self):
        rows = drama('a', [101, 102, 103])
        report = self.root / 'retry.json'
        report.write_text(json.dumps({'project_ref': PROJECT, 'pending_message_ids': [101]}))
        selected, deferred, evaluations = retry_plan_dramas(
            rows, self.root, report, PROJECT, quality_policy=POLICY,
            archived={101}, covered={101}, video_limit=1)
        self.assertEqual(mids(selected), [102, 103])
        self.assertEqual(deferred, [])
        self.assertEqual(evaluations[0]['committed_message_ids'], [101])
        with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
            retry_plan_dramas(rows, self.root, report, 'b'*20, quality_policy=POLICY)


class DramaPassTests(unittest.IsolatedAsyncioTestCase):
    async def run_pass(self, rows, *, archived=(), covered=(), only=(), handler=None,
                       retry_rounds=0, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            jobs, deferred, evaluations = plan_dramas(
                rows, tmp, set(archived), quality_policy=POLICY, covered=set(covered), only=only)
            calls = []
            async def process(*args, **options):
                batch = args[7]
                calls.append((mids(batch), options))
                return handler(batch, options, len(calls)) if handler else segment(batch)
            with patch('video_archive.pipeline.batch.process_batch', process), \
                    patch('video_archive.pipeline.batch.asyncio.sleep', new=AsyncMock()):
                report = await process_drama_pass(
                    None, None, None, None, None, None, None, jobs,
                    drama_evaluations=evaluations, deferred=deferred,
                    report_path=Path(tmp) / 'pass.json', project_ref=PROJECT,
                    quality_policy_summary=POLICY.to_dict(), retry_rounds=retry_rounds, **kwargs)
            saved = json.loads((Path(tmp) / 'pass.json').read_text())
            self.assertEqual(saved, report)
            return report, calls

    async def test_large_drama_finishes_100_plus_remainder_before_next_drama(self):
        report, calls = await self.run_pass(drama('a', range(101, 257)) + drama('b', [300, 301]))
        self.assertEqual([len(call[0]) for call in calls], [100, 56, 2])
        self.assertEqual([part['drama_id'] for part in report['segments']], ['a', 'a', 'b'])
        self.assertEqual(report['status'], 'all_archived')
        self.assertTrue(all(e['state'] == 'complete' for e in report['dramas']))
        self.assertEqual(report['quality_policy'], POLICY.to_dict())

    async def test_500_video_drama_fits_one_explicit_500_segment(self):
        report, calls = await self.run_pass(drama('a', range(101, 601)), batch_size=500)
        self.assertEqual([len(call[0]) for call in calls], [500])
        self.assertEqual(report['dramas'][0]['state'], 'complete')

    async def test_over_500_video_drama_finishes_both_segments_before_next_drama(self):
        report, calls = await self.run_pass(
            drama('a', range(101, 602)) + drama('b', [700]), batch_size=500)
        self.assertEqual([len(call[0]) for call in calls], [500, 1, 1])
        self.assertEqual([part['drama_id'] for part in report['segments']], ['a', 'a', 'b'])
        self.assertEqual(report['status'], 'all_archived')

    async def test_single_failure_does_not_block_next_drama_then_retries_by_drama(self):
        def handler(batch, options, number):
            return segment(batch, failures={101} if number == 1 else ())
        report, calls = await self.run_pass(
            drama('a', [101, 102]) + drama('b', [103]), handler=handler, retry_rounds=1)
        self.assertEqual([ids for ids, _ in calls], [[101, 102], [103], [101]])
        self.assertTrue(calls[-1][1]['resume_partials'])
        self.assertEqual(report['status'], 'all_archived')
        self.assertEqual(report['segments'][-1]['retry_round'], 1)

    async def test_cover_failure_retry_does_not_reread_successful_video(self):
        def handler(batch, options, number):
            if number == 1:
                return segment(batch, cover_failures={101})
            self.assertEqual(options['cover_only'], {101})
            result = segment(batch)
            result['videos'][0]['result'] = 'video_already_committed'
            return result
        report, calls = await self.run_pass(drama('a', [101, 102]), handler=handler, retry_rounds=1)
        self.assertEqual([ids for ids, _ in calls], [[101, 102], [101]])
        self.assertEqual(report['dramas'][0]['state'], 'complete')
        self.assertTrue(report['dramas'][0]['cover_complete'])

    async def test_failed_member_is_not_a_complete_drama(self):
        report, _ = await self.run_pass(drama('a', [101, 102]),
                                        handler=lambda batch, *_: segment(batch, failures={102}))
        self.assertEqual(report['status'], 'incomplete')
        self.assertFalse(report['dramas'][0]['video_complete'])
        self.assertEqual(report['dramas'][0]['remaining_message_ids'], [102])

    async def test_cover_failure_alone_keeps_whole_drama_incomplete(self):
        report, _ = await self.run_pass(drama('a', [101, 102]),
                                        handler=lambda batch, *_: segment(batch, cover_failures={102}))
        self.assertEqual(report['status'], 'incomplete')
        self.assertTrue(report['dramas'][0]['video_complete'])
        self.assertFalse(report['dramas'][0]['cover_complete'])

    async def test_hard_stop_prevents_starting_next_drama(self):
        report, calls = await self.run_pass(
            drama('a', [101, 102]) + drama('b', [103]),
            handler=lambda batch, *_: segment(batch, failures={102}, stop='disk_full'), retry_rounds=1)
        self.assertEqual([ids for ids, _ in calls], [[101, 102]])
        self.assertEqual(report['status'], 'stopped')
        self.assertEqual(report['stop_reason'], 'disk_full')
        self.assertEqual(report['dramas'][1]['state'], 'queued')
        self.assertIn(103, report['pending_message_ids'])

    async def test_previous_commits_complement_current_work_without_redundant_downloads(self):
        report, calls = await self.run_pass(drama('a', [101, 102, 103]),
                                            archived={101, 102}, covered={101, 102})
        self.assertEqual([ids for ids, _ in calls], [[103]])
        self.assertEqual(report['dramas'][0]['expected_message_ids'], [101, 102, 103])
        self.assertEqual(report['dramas'][0]['state'], 'complete')

    async def test_explicit_readback_failure_is_not_masked_by_older_commit(self):
        report, calls = await self.run_pass(drama('a', [101, 102]),
            archived={101, 102}, covered={101, 102}, only=[101], cover_only={101},
            handler=lambda batch, *_: segment(batch, failures={101}))
        self.assertEqual(calls[0][1]['cover_only'], set())
        self.assertEqual(report['dramas'][0]['state'], 'incomplete')
        self.assertFalse(report['dramas'][0]['video_complete'])

    async def test_retry_sweeps_remain_bounded_even_when_all_files_fail(self):
        report, calls = await self.run_pass(
            drama('a', [101]) + drama('b', [102]), retry_rounds=2,
            handler=lambda batch, *_: segment(batch, failures=mids(batch)))
        self.assertEqual([ids for ids, _ in calls], [[101], [102], [101], [102], [101], [102]])
        self.assertEqual(report['status'], 'incomplete')

    async def test_truncating_planned_jobs_is_rejected_before_any_transfer(self):
        with tempfile.TemporaryDirectory() as tmp:
            selected, _, evaluations = plan_dramas(
                drama('a', [101, 102]), tmp, set(), quality_policy=POLICY)
            with patch('video_archive.pipeline.batch.process_batch', new=AsyncMock()) as process:
                with self.assertRaisesRegex(PipelineError, 'identity_conflict'):
                    await process_drama_pass(None, None, None, None, None, None, None,
                                             selected[:1], drama_evaluations=evaluations)
            process.assert_not_called()

    async def test_empty_selection_never_connects_telegram(self):
        rows = drama('a', [101, 102]); rows[1][2].update(width=1280, height=720)
        with tempfile.TemporaryDirectory() as tmp:
            selected, deferred, evaluations = plan_dramas(rows, tmp, set(), quality_policy=POLICY)
            with patch('video_archive.pipeline.batch.archive.client_for') as client:
                report = await run_batch(None, None, None, None, None, selected,
                    drama_evaluations=evaluations, deferred=deferred)
            client.assert_not_called()
            self.assertEqual(report['dramas'][0]['state'], 'rejected')
            self.assertEqual(report['selected_drama_count'], 0)


if __name__ == '__main__':
    unittest.main()
