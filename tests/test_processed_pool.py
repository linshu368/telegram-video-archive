"""Terminal source topics remain skipped until an explicit reopen; no services."""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.pipeline.common import PipelineError, identity
from video_archive.pipeline.completion import manifest_digest, manifest_for_jobs
from video_archive.pipeline.processed import (filter_unprocessed_rows, load_pool, processed_file,
                                              record_evaluations, record_success, reopen)
from video_archive.screening.quality import QualityPolicy, evaluate_drama


PROJECT, OTHER_PROJECT = 'a' * 20, 'b' * 20
SUCCESS = {'delivery_accepted': True, 'ai_complete': True, 'transcode_complete': True,
           'cover_complete': True, 'expected_count': 2, 'verified_video_count': 2,
           'missing_video_count': 0}


def rows(topic=20, mids=(101, 102), source=-100123):
    did = identity('topic', source, topic)
    return [({'id': did, 'source_id': str(source), 'topic_id': str(topic), 'title': f'Drama {topic}'},
             {'id': identity('message', source, mid), 'drama_id': did},
             {'message_id': mid, 'source': {'id': source}, 'topic_id': topic,
              'media_id': 9000 + mid, 'expected_size': 2_500_000, 'duration_seconds': 10,
              'width': 720, 'height': 1280, 'bitrate_bps': 2_000_000}, {'status': 'pending'})
            for mid in mids]


def evaluation(jobs, *, screening_rejected=False, download_rejected=False):
    result = evaluate_drama(jobs, QualityPolicy())
    result['expected_manifest'] = manifest_for_jobs(jobs)
    if screening_rejected:
        result.update(decision='reject', reason='below_minimum_quality')
    if download_rejected:
        result.update(delivery_finalized=True, delivery_accepted=False,
                      delivery_decision='rejected_incomplete', expected_count=len(jobs),
                      verified_video_count=1, missing_video_count=len(jobs) - 1)
    return result


class ProcessedPoolTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.path = processed_file(self.root, PROJECT)
        self.jobs = rows()
        self.did = self.jobs[0][0]['id']

    def tearDown(self):
        self.tmp.cleanup()

    def reject(self, jobs=None, **kwargs):
        return record_evaluations(self.path, PROJECT,
            [evaluation(jobs or self.jobs, screening_rejected=True)], **kwargs)

    def test_path_and_empty_pool_are_project_scoped(self):
        self.assertEqual(self.path, self.root / 'pipeline/projects' / PROJECT / 'processed-dramas.json')
        self.assertEqual(load_pool(self.path, PROJECT), {'version': 1, 'project_ref': PROJECT, 'dramas': {}})
        self.reject()
        other = processed_file(self.root, OTHER_PROJECT)
        remaining, skipped = filter_unprocessed_rows(other, OTHER_PROJECT, self.jobs)
        self.assertEqual(remaining, self.jobs)
        self.assertEqual(skipped, [])
        with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
            load_pool(self.path, OTHER_PROJECT)

    def test_quality_rejection_records_identity_reason_manifest_and_report(self):
        marked = self.reject(report_path='reports/source-screening.json')
        entry = load_pool(self.path, PROJECT)['dramas'][self.did]
        self.assertEqual(marked[0], entry)
        self.assertEqual(entry['status'], 'screening_rejected')
        self.assertTrue(entry['terminal'])
        self.assertEqual((entry['source_id'], entry['topic_id']), ('-100123', '20'))
        self.assertEqual(entry['reason'], 'below_minimum_quality')
        self.assertEqual(entry['manifest_digest'], manifest_digest(manifest_for_jobs(self.jobs)))
        self.assertEqual(entry['report_path'], 'reports/source-screening.json')
        remaining, skipped = filter_unprocessed_rows(self.path, PROJECT, self.jobs)
        self.assertEqual(remaining, [])
        self.assertEqual(skipped[0]['video_count'], 2)
        self.assertEqual(skipped[0]['drama_id'], self.did)

    def test_final_download_rejection_is_terminal(self):
        marked = record_evaluations(self.path, PROJECT,
                                    [evaluation(self.jobs, download_rejected=True)])
        self.assertEqual(marked[0]['status'], 'download_rejected')
        self.assertEqual(filter_unprocessed_rows(self.path, PROJECT, self.jobs)[0], [])

    def test_pending_temporary_failures_and_accepted_delivery_are_not_terminal(self):
        base = evaluation(self.jobs)
        candidates = []
        for changes in (
                {'decision': 'pending', 'reason': 'missing_video_metadata'},
                {'decision': 'reject', 'reason': 'missing_video_metadata'},
                {'planning_status': 'deferred', 'planning_reason': 'drama_member_deferred'},
                {'delivery_finalized': False, 'delivery_accepted': False,
                 'delivery_decision': 'rejected_incomplete'},
                {'delivery_finalized': True, 'delivery_accepted': False,
                 'delivery_decision': 'pending'},
                {'delivery_finalized': True, 'delivery_accepted': True,
                 'delivery_decision': 'complete'},
                {'delivery_finalized': True, 'delivery_accepted': True,
                 'delivery_decision': 'accepted_with_gaps'}):
            candidate = copy.deepcopy(base); candidate.update(changes); candidates.append(candidate)
        for flag in ('delivery_stopped', 'delivery_interrupted'):
            candidate = evaluation(self.jobs, download_rejected=True)
            candidate[flag] = True
            candidates.append(candidate)
        self.assertEqual(record_evaluations(self.path, PROJECT, candidates), [])
        self.assertEqual(load_pool(self.path, PROJECT)['dramas'], {})

    def test_delivery_acceptance_without_ai_transcode_and_cover_confirmation_is_not_success(self):
        for flag in ('delivery_accepted', 'ai_complete', 'transcode_complete', 'cover_complete'):
            for value in (False, None, 1):
                with self.subTest(flag=flag, value=value):
                    details = {**SUCCESS, flag: value}
                    self.assertIsNone(record_success(self.path, PROJECT, self.jobs, details=details))
        self.assertIsNone(record_success(self.path, PROJECT, self.jobs))
        self.assertEqual(load_pool(self.path, PROJECT)['dramas'], {})

    def test_confirmed_pipeline_success_records_completed(self):
        entry = record_success(self.path, PROJECT, self.jobs, details=SUCCESS,
                               report_path='reports/ai-final.json')
        self.assertEqual(entry['status'], 'completed')
        self.assertEqual(entry['details']['ai_complete'], True)
        self.assertEqual(entry['expected_message_ids'], [101, 102])
        self.assertEqual(entry['report_path'], 'reports/ai-final.json')
        self.assertEqual(filter_unprocessed_rows(self.path, PROJECT, self.jobs)[0], [])

    def test_new_members_do_not_automatically_reopen_same_source_topic(self):
        self.reject()
        original = self.path.read_bytes()
        expanded = rows(mids=(101, 102, 103))
        remaining, skipped = filter_unprocessed_rows(self.path, PROJECT, expanded)
        self.assertEqual(remaining, [])
        self.assertEqual((skipped[0]['recorded_video_count'], skipped[0]['video_count']), (2, 3))
        self.assertTrue(skipped[0]['manifest_changed'])
        record_evaluations(self.path, PROJECT, [evaluation(expanded, screening_rejected=True)],
                           report_path='reports/new-scan.json')
        self.assertEqual(self.path.read_bytes(), original)

    def test_repeated_terminal_record_is_idempotent_and_preserves_first_timestamp(self):
        first = self.reject(report_path='reports/first.json')[0]
        saved = self.path.read_bytes()
        later = self.reject(report_path='reports/later.json')[0]
        self.assertEqual(first, later)
        self.assertEqual(len(later['history']), 1)
        self.assertEqual(self.path.read_bytes(), saved)

    def test_other_source_or_topic_is_independent(self):
        self.reject()
        others = rows(topic=21, mids=(201, 202)) + rows(source=-100456, mids=(101, 102))
        remaining, skipped = filter_unprocessed_rows(self.path, PROJECT, self.jobs + others)
        self.assertEqual(remaining, others)
        self.assertEqual(len(skipped), 1)

    def test_reused_drama_id_cannot_hide_a_different_source_topic(self):
        self.reject()
        changed = rows(topic=21)
        for d, v, _, _ in changed:
            d['id'] = self.did; v['drama_id'] = self.did
        with self.assertRaisesRegex(PipelineError, 'identity_conflict'):
            filter_unprocessed_rows(self.path, PROJECT, changed)

    def test_reopen_one_drama_preserves_reason_and_history_and_allows_processing(self):
        other = rows(topic=21, mids=(201, 202))
        self.reject(); self.reject(other)
        original = load_pool(self.path, PROJECT)['dramas'][self.did]
        reopened = reopen(self.path, PROJECT, [self.did])
        self.assertEqual(len(reopened), 1)
        self.assertFalse(reopened[0]['terminal'])
        self.assertEqual(reopened[0]['status'], 'reopened')
        self.assertEqual(reopened[0]['reason'], original['reason'])
        self.assertEqual([event['action'] for event in reopened[0]['history']], ['marked', 'reopened'])
        remaining, skipped = filter_unprocessed_rows(self.path, PROJECT, self.jobs + other)
        self.assertEqual(remaining, self.jobs)
        self.assertEqual(len(skipped), 1)
        completed = record_success(self.path, PROJECT, self.jobs, details=SUCCESS)
        self.assertEqual(completed['status'], 'completed')
        self.assertEqual([event['action'] for event in completed['history']], ['marked', 'reopened', 'marked'])

    def test_reopen_all_and_empty_id_selection_have_explicit_meanings(self):
        self.reject(); self.reject(rows(topic=21, mids=(201, 202)))
        saved = self.path.read_bytes()
        self.assertEqual(reopen(self.path, PROJECT, []), [])
        self.assertEqual(self.path.read_bytes(), saved)
        self.assertEqual(len(reopen(self.path, PROJECT, None)), 2)
        self.assertEqual(reopen(self.path, PROJECT, None), [])

    def test_success_rejects_partial_or_multiple_drama_input(self):
        with self.assertRaisesRegex(PipelineError, 'manifest_conflict'):
            record_success(self.path, PROJECT, self.jobs[:1], details=SUCCESS)
        with self.assertRaisesRegex(PipelineError, 'requires_one_drama'):
            record_success(self.path, PROJECT, self.jobs + rows(topic=21, mids=(201, 202)), details=SUCCESS)
        self.assertEqual(load_pool(self.path, PROJECT)['dramas'], {})

    def test_missing_or_duplicate_source_manifest_cannot_be_sealed(self):
        legacy = evaluation(self.jobs, screening_rejected=True); legacy.pop('expected_manifest')
        self.assertEqual(record_evaluations(self.path, PROJECT, [legacy]), [])
        duplicated = evaluation(self.jobs, screening_rejected=True)
        duplicated['expected_manifest'].append(dict(duplicated['expected_manifest'][0]))
        with self.assertRaisesRegex(PipelineError, 'duplicate_identity'):
            record_evaluations(self.path, PROJECT, [duplicated])
        self.assertEqual(load_pool(self.path, PROJECT)['dramas'], {})

    def test_invalid_project_and_mismatched_pool_are_rejected(self):
        for invalid in ('', '../main', 'bad/ref', 'bad\\ref', 'x'*65, 'A'*20):
            with self.subTest(project=invalid), self.assertRaisesRegex(PipelineError, 'project_invalid'):
                processed_file(self.root, invalid)
        self.reject()
        with self.assertRaisesRegex(PipelineError, 'project_mismatch'):
            record_success(self.path, OTHER_PROJECT, self.jobs, details=SUCCESS)

    def test_pool_and_atomic_temporary_symlinks_are_rejected(self):
        target = self.root / 'untouched.json'; target.write_text('original')
        self.path.parent.mkdir(parents=True)
        self.path.symlink_to(target)
        with self.assertRaisesRegex(PipelineError, 'symlink'):
            self.reject()
        self.path.unlink()
        self.path.with_suffix('.json.tmp').symlink_to(target)
        with self.assertRaisesRegex(PipelineError, 'symlink'):
            self.reject()
        self.assertEqual(target.read_text(), 'original')

    def test_symlinked_storage_parent_is_rejected(self):
        external = self.root / 'external'; external.mkdir()
        (self.root / 'pipeline').symlink_to(external, target_is_directory=True)
        with self.assertRaisesRegex(PipelineError, 'symlink'):
            processed_file(self.root, PROJECT)

    def test_invalid_completed_record_without_ai_evidence_does_not_load(self):
        self.reject()
        data = json.loads(self.path.read_text())
        data['dramas'][self.did]['status'] = 'completed'
        self.path.write_text(json.dumps(data))
        with self.assertRaisesRegex(PipelineError, 'success_unconfirmed'):
            load_pool(self.path, PROJECT)


if __name__ == '__main__':
    unittest.main()
