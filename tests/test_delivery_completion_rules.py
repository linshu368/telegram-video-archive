"""Final delivery thresholds and lane recovery without external services."""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from video_archive.core import atomic_json
from video_archive.pipeline.common import PipelineError
from video_archive.pipeline.completion import drama_progress, recover_active_segment


VIDEO_SUCCESS = {'uploaded_readback_verified', 'video_already_committed'}
COVER_SUCCESS = {'cover_uploaded_readback_verified', 'already_archived'}
PROJECT = 'a' * 20


def sample(count=50, missing=0, *, did='drama-a', start=1):
    members = [{'source_id': '-100123', 'message_id': mid, 'media_id': str(9000 + mid),
                'video_id': f'video-{mid}', 'expected_size': 100, 'topic_id': '100',
                'drama_id': did} for mid in range(start, start + count)]
    ids = [member['message_id'] for member in members]
    evaluation = {'drama_id': did, 'decision': 'accept', 'planning_status': 'scheduled',
                  'expected_message_ids': ids, 'scheduled_message_ids': list(ids),
                  'committed_message_ids': [], 'complete_message_ids': [],
                  'cover_only_message_ids': [], 'expected_manifest': members,
                  'delivery_finalized': True}
    missing_ids = set(ids[-missing:]) if missing else set()
    part = {'project_ref': PROJECT, 'drama_id': did, 'expected_manifest': copy.deepcopy(members),
            'videos': [{**member,
                        'result': 'failed' if member['message_id'] in missing_ids else 'uploaded_readback_verified',
                        'cover_result': 'cover_uploaded_readback_verified',
                        **({'error': 'download_timeout'} if member['message_id'] in missing_ids else {})}
                       for member in members]}
    return evaluation, part


def progress(evaluation, parts):
    return drama_progress(evaluation, parts, VIDEO_SUCCESS, COVER_SUCCESS)


class DeliveryRulesTests(unittest.TestCase):
    def test_fifty_members_allow_four_missing_but_not_five(self):
        for missing, decision in ((4, 'accepted_with_gaps'), (5, 'rejected_incomplete')):
            with self.subTest(missing=missing):
                evaluation, part = sample(missing=missing)
                result = progress(evaluation, [part])
                self.assertEqual(result['delivery_decision'], decision)
                self.assertEqual(result['delivery_accepted'], missing == 4)
                self.assertEqual(result['expected_count'], 50)
                self.assertEqual(result['missing_video_count'], missing)
                self.assertFalse(result['video_complete'])

    def test_strict_boundary_does_not_round_small_drama_counts(self):
        for count, decision in ((11, 'accepted_with_gaps'), (10, 'rejected_incomplete')):
            with self.subTest(count=count):
                evaluation, part = sample(count, missing=1)
                self.assertEqual(progress(evaluation, [part])['delivery_decision'], decision)

    def test_complete_videos_and_failed_covers_have_separate_outcomes(self):
        evaluation, part = sample()
        part['videos'][-1]['cover_result'] = 'failed'
        result = progress(evaluation, [part])
        self.assertEqual(result['delivery_decision'], 'complete')
        self.assertTrue(result['delivery_accepted'])
        self.assertTrue(result['video_complete'])
        self.assertFalse(result['cover_complete'])
        self.assertEqual(result['missing_video_count'], 0)
        self.assertEqual(result['remaining_cover_message_ids'], [50])

    def test_unfinalized_or_stopped_delivery_never_uses_the_gap_allowance(self):
        for flags in ({'delivery_finalized': False}, {'delivery_stopped': True},
                      {'delivery_interrupted': True}):
            with self.subTest(flags=flags):
                evaluation, part = sample(missing=1)
                evaluation.update(flags)
                result = progress(evaluation, [part])
                self.assertEqual(result['delivery_decision'], 'pending')
                self.assertFalse(result['delivery_accepted'])

    def test_unattempted_unknown_and_deferred_work_stays_pending(self):
        cases = [(status, None) for status in ('queued', 'not_started', 'interrupted',
                                              'downloading', 'stopped', 'unknown')]
        cases += [('retry_deferred', error) for error in (
            'lease_active', 'database_backoff', 'task_not_claimable')]
        cases += [('failed', error) for error in (
            'lease_active', 'database_backoff', 'task_not_claimable')]
        for status, error in cases:
            with self.subTest(status=status, error=error):
                evaluation, part = sample(missing=1)
                part['videos'][-1].update(result=status, error=error)
                result = progress(evaluation, [part])
                self.assertEqual(result['delivery_decision'], 'pending')
                self.assertFalse(result['delivery_accepted'])

    def test_exhausted_attempts_are_a_finalized_gap(self):
        evaluation, part = sample(missing=1)
        part['videos'][-1].update(result='retry_deferred', error='attempts_exhausted')
        self.assertEqual(progress(evaluation, [part])['delivery_decision'], 'accepted_with_gaps')

    def test_quality_rejection_and_deferred_plans_are_not_deliverable(self):
        for status, decision in (('rejected', 'reject'), ('pending', 'pending'), ('deferred', 'accept')):
            with self.subTest(status=status):
                evaluation, part = sample(missing=1)
                evaluation.update(planning_status=status, decision=decision)
                self.assertEqual(progress(evaluation, [part])['delivery_decision'], 'pending')

    def test_preverified_cloud_members_need_no_new_receipt(self):
        evaluation, _ = sample()
        evaluation.update(planning_status='already_complete', scheduled_message_ids=[],
                          committed_message_ids=list(range(1, 51)),
                          complete_message_ids=list(range(1, 51)))
        result = progress(evaluation, [])
        self.assertEqual(result['delivery_decision'], 'complete')
        self.assertEqual(result['verified_video_count'], 50)

    def test_duplicate_expected_members_and_receipts_cannot_replace_a_video(self):
        evaluation, part = sample()
        evaluation['expected_message_ids'].append(1)
        with self.assertRaisesRegex(PipelineError, 'duplicate_identity'):
            progress(evaluation, [part])
        evaluation, part = sample()
        part['videos'][-1] = dict(part['videos'][-2])
        with self.assertRaisesRegex(PipelineError, 'duplicate_id'):
            progress(evaluation, [part])

    def test_repeated_segments_cannot_count_an_unseen_member_as_present(self):
        evaluation, part = sample()
        part['videos'].pop()
        result = progress(evaluation, [part, copy.deepcopy(part)])
        self.assertEqual(result['verified_video_count'], 49)
        self.assertEqual(result['missing_video_count'], 1)
        self.assertEqual(result['delivery_decision'], 'pending')

    def test_wrong_identity_is_rejected_and_missing_legacy_identity_is_pending(self):
        evaluation, part = sample(missing=1)
        part['videos'][0]['media_id'] = '99999'
        with self.assertRaisesRegex(PipelineError, 'identity_conflict'):
            progress(evaluation, [part])
        evaluation, part = sample()
        del part['videos'][0]['source_id']
        result = progress(evaluation, [part])
        self.assertTrue(result['video_complete'])
        self.assertEqual(result['delivery_decision'], 'pending')
        self.assertEqual(result['completion_identity_status'], 'legacy_result_identity_missing')

    def test_new_verified_receipts_can_supersede_legacy_failure_evidence(self):
        evaluation, old = sample(missing=1)
        del old['videos'][-1]['source_id']
        _, current = sample()
        result = progress(evaluation, [old, current])
        self.assertEqual(result['delivery_decision'], 'complete')
        self.assertTrue(result['failure_history'])

    def test_ratio_configuration_is_exact_and_invalid_policy_fails(self):
        evaluation, part = sample(missing=4)
        evaluation['max_missing_ratio'] = '.08'
        self.assertEqual(progress(evaluation, [part])['delivery_decision'], 'rejected_incomplete')
        for invalid in ('NaN', 'Infinity', -.1, 1.1, True):
            with self.subTest(value=invalid):
                evaluation['max_missing_ratio'] = invalid
                with self.assertRaisesRegex(PipelineError, 'invalid_completion_policy'):
                    progress(evaluation, [part])


class LaneRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.path = self.root / 'pass.json'
        self.a, self.part_a = sample(2, did='a')
        self.b, self.part_b = sample(2, did='b', start=101)
        self.child_a, self.child_b = self.root / 'pass-lane-a.json', self.root / 'pass-lane-b.json'
        atomic_json(self.child_a, self.part_a)
        atomic_json(self.child_b, self.part_b)
        self.report = {'project_ref': PROJECT, 'status': 'interrupted', 'segments': [],
                       'dramas': [self.a, self.b], 'active_segment_reports': {
                           'a': {'path': self.child_a.name, 'message_ids': [1, 2],
                                 'drama_id': 'a', 'retry_round': 0},
                           'b': {'path': self.child_b.name, 'message_ids': [101, 102],
                                 'drama_id': 'b', 'retry_round': 1}}}

    def tearDown(self):
        self.temp.cleanup()

    def test_all_lane_reports_recover_once_with_legacy_pointer_compatibility(self):
        self.report.update(active_segment_report=str(self.child_a),
                           active_segment_message_ids=[1, 2], active_segment_drama_id='a')
        self.assertTrue(recover_active_segment(self.report, self.path, PROJECT))
        self.assertEqual(len(self.report['segments']), 2)
        self.assertEqual(self.report['segments'][1]['retry_round'], 1)
        self.assertEqual({part['drama_id'] for part in self.report['segments']}, {'a', 'b'})
        self.assertFalse(recover_active_segment(self.report, self.path, PROJECT))
        self.assertEqual(len(self.report['segments']), 2)

    def test_other_project_and_cross_lane_members_are_rejected(self):
        bad = copy.deepcopy(self.part_b); bad['project_ref'] = 'b' * 20
        atomic_json(self.child_b, bad)
        with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
            recover_active_segment(self.report, self.path, PROJECT)
        atomic_json(self.child_b, self.part_b)
        self.report['active_segment_reports']['b']['message_ids'] = [1, 2]
        with self.assertRaisesRegex(PipelineError, 'unexpected_id'):
            recover_active_segment(self.report, self.path, PROJECT)

    def test_child_receipt_identity_and_duplicate_ids_are_validated(self):
        bad = copy.deepcopy(self.part_b); bad['videos'][0]['media_id'] = '999999'
        atomic_json(self.child_b, bad)
        with self.assertRaisesRegex(PipelineError, 'identity_conflict'):
            recover_active_segment(self.report, self.path, PROJECT)
        bad = copy.deepcopy(self.part_b); bad['videos'][-1] = dict(bad['videos'][0])
        atomic_json(self.child_b, bad)
        with self.assertRaisesRegex(PipelineError, 'duplicate_id'):
            recover_active_segment(self.report, self.path, PROJECT)

    def test_missing_legacy_project_marker_cannot_supply_completion_evidence(self):
        del self.part_b['project_ref']; atomic_json(self.child_b, self.part_b)
        self.assertTrue(recover_active_segment(self.report, self.path, PROJECT))
        self.assertEqual(len(self.report['segments']), 1)
        self.assertEqual(self.report['active_segment_recoveries']['b'], 'legacy_segment_project_missing')

    def test_child_symlinks_and_one_child_claimed_by_two_lanes_are_rejected(self):
        self.child_b.unlink(); self.child_b.symlink_to(self.child_a)
        with self.assertRaisesRegex(PipelineError, 'path_conflict'):
            recover_active_segment(self.report, self.path, PROJECT)
        self.child_b.unlink(); atomic_json(self.child_b, self.part_b)
        self.report['active_segment_reports']['b']['path'] = self.child_a.name
        with self.assertRaisesRegex(PipelineError, 'drama_conflict'):
            recover_active_segment(self.report, self.path, PROJECT)


if __name__ == '__main__':
    unittest.main()
