"""A whole drama enters later stages only after an exact-manifest delivery decision."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.pipeline.common import PipelineError
from video_archive.pipeline.completion import manifest_for_jobs
from video_archive.pipeline.outcomes import filter_approved_rows, write_outcomes
from test_drama_scheduling import drama


PROJECT = 'a' * 20


def evaluation(rows, *, decision='complete', accepted=True, finalized=True):
    return {'drama_id': rows[0][1]['drama_id'], 'decision': 'accept',
            'expected_manifest': manifest_for_jobs(rows), 'delivery_accepted': accepted,
            'delivery_finalized': finalized, 'delivery_decision': decision, 'state': decision}


class OutcomeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'progress/drama-outcomes.json'
        self.rows = drama('a', range(101, 151))

    def tearDown(self):
        self.tmp.cleanup()

    def test_unassessed_or_pending_drama_does_not_enter_later_stages(self):
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows), [])
        write_outcomes(self.path, PROJECT, [evaluation(self.rows, finalized=False)])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows), [])

    def test_matching_finalized_drama_passes_and_changed_members_do_not(self):
        write_outcomes(self.path, PROJECT, [evaluation(self.rows)])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows), self.rows)
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows[:-1]), [])
        changed = [(*job[:2], {**job[2], 'media_id': 123}, job[3]) if i == 0 else job
                   for i, job in enumerate(self.rows)]
        self.assertEqual(filter_approved_rows(self.path, PROJECT, changed), [])

    def test_accepted_gaps_are_explicit_and_rejected_whole_drama_is_excluded(self):
        entry = evaluation(self.rows, decision='accepted_with_gaps')
        entry.update(verified_video_count=46, missing_video_count=4, missing_video_ratio=.08,
                     remaining_video_message_ids=[147, 148, 149, 150])
        write_outcomes(self.path, PROJECT, [entry])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows), self.rows)
        stored = json.loads(self.path.read_text())['dramas']['a']
        self.assertEqual((stored['expected_count'], stored['missing_video_count']), (50, 4))
        write_outcomes(self.path, PROJECT, [evaluation(self.rows, decision='rejected_incomplete', accepted=False)])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows), [])

    def test_updating_one_drama_does_not_forget_another_and_repair_blocks_until_final(self):
        other = drama('b', [201, 202])
        write_outcomes(self.path, PROJECT, [evaluation(self.rows), evaluation(other)])
        write_outcomes(self.path, PROJECT, [evaluation(self.rows, accepted=False, finalized=False)])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows + other), other)
        write_outcomes(self.path, PROJECT, [evaluation(self.rows)])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows + other), self.rows + other)

    def test_other_project_or_legacy_report_cannot_approve_delivery(self):
        write_outcomes(self.path, PROJECT, [evaluation(self.rows)])
        with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
            filter_approved_rows(self.path, 'b' * 20, self.rows)
        legacy = {'drama_id': 'a', 'delivery_accepted': True, 'state': 'complete'}
        write_outcomes(self.path, PROJECT, [legacy])
        self.assertEqual(filter_approved_rows(self.path, PROJECT, self.rows), [])

    def test_outcome_symlink_is_not_followed(self):
        target = Path(self.tmp.name) / 'untouched.json'
        target.write_text('unchanged')
        self.path.parent.mkdir()
        self.path.symlink_to(target)
        with self.assertRaisesRegex(PipelineError, 'symlink'):
            write_outcomes(self.path, PROJECT, [evaluation(self.rows)])
        self.assertEqual(target.read_text(), 'unchanged')
