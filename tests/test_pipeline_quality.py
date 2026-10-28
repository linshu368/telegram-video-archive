"""Metadata-only quality decisions: complete dramas, no network or database."""
import json
from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.pipeline.common import PipelineError
from video_archive.screening.quality import QualityPolicy, evaluate_drama, media_metadata


def job(mid, width=720, height=1280, bitrate=2_000_000, did='drama-1'):
    return ({'id': did}, {'id': f'video-{mid}', 'drama_id': did},
            {'message_id': mid, 'width': width, 'height': height, 'bitrate_bps': bitrate}, {})


class MetadataTests(unittest.TestCase):
    def test_existing_fields_win_and_source_size_is_used_before_local_size(self):
        record = {'width': 720, 'height': 1280, 'bitrate_bps': 2_000_000,
                  'expected_size': 100, 'duration_seconds': 1}
        self.assertEqual(media_metadata(record, {'actual_size': 200}),
                         {'width': 720, 'height': 1280, 'bitrate_bps': 2_000_000})
        record.pop('bitrate_bps')
        self.assertEqual(media_metadata(record, {'actual_size': 200})['bitrate_bps'], 800)

    def test_average_bitrate_uses_half_up_rounding_and_int32_bound(self):
        self.assertEqual(media_metadata({'expected_size': 3, 'duration_seconds': 16})['bitrate_bps'], 2)
        self.assertIsNone(media_metadata({'expected_size': 2**40, 'duration_seconds': 1})['bitrate_bps'])
        self.assertIsNone(media_metadata({'expected_size': 1, 'duration_seconds': 100})['bitrate_bps'])

    def test_verified_local_metadata_fills_missing_source_declarations(self):
        state = {'actual_size': 100, 'probe': {'duration': '.1', 'video_streams': [
            {'width': 1080, 'height': 1920}]}}
        self.assertEqual(media_metadata({}, state),
                         {'width': 1080, 'height': 1920, 'bitrate_bps': 8000})

    def test_invalid_numeric_values_stay_unknown(self):
        for value in (True, 0, -1, 'NaN', 'Infinity', 'junk', 1.5):
            result = media_metadata({'width': value, 'height': value, 'bitrate_bps': value})
            self.assertEqual(result, {'width': None, 'height': None, 'bitrate_bps': None})
        self.assertIsNone(media_metadata({'expected_size': 100, 'duration_seconds': float('nan')})['bitrate_bps'])


class PolicyTests(unittest.TestCase):
    def test_policy_is_frozen_validated_and_json_serializable(self):
        policy = QualityPolicy(resolution_tolerance=Decimal('.1'))
        json.dumps(policy.to_dict())
        with self.assertRaises(FrozenInstanceError):
            policy.minimum_grade = 1
        for kwargs in ({'minimum_grade': 0}, {'minimum_grade': True},
                       {'resolution_tolerance': 1}, {'resolution_tolerance': -1},
                       {'stable_bitrate_ratio': .9}, {'majority_share': .5},
                       {'majority_share': 1.1}, {'max_low_share': .5},
                       {'max_low_share': -.1}, {'majority_share': float('nan')}):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(PipelineError, 'invalid_quality_policy'):
                QualityPolicy(**kwargs)

    def test_empty_inventory_is_pending(self):
        result = evaluate_drama([])
        self.assertEqual((result['decision'], result['reason']), ('pending', 'empty_drama'))

    def test_known_landscape_or_square_rejects_even_with_other_missing_metadata(self):
        unknown = job(1, bitrate=None)
        for width, height in ((1280, 720), (720, 720)):
            result = evaluate_drama([unknown, job(2, width, height)])
            self.assertEqual(result['decision'], 'reject')
            self.assertEqual(result['reason'], 'non_portrait_video')
            self.assertEqual(result['missing_metadata_count'], 1)

    def test_missing_metadata_keeps_whole_drama_pending(self):
        for key in ('width', 'height', 'bitrate_bps'):
            rows = [job(1), job(2)]
            rows[1][2][key] = None
            result = evaluate_drama(rows)
            self.assertEqual(result['decision'], 'pending')
            self.assertEqual(result['expected_message_ids'], [1, 2])

    def test_video_payload_metadata_can_fill_source_record(self):
        row = job(1)
        row[1].update(width=720, height=1280, bitrate_bps=2_000_000)
        for key in ('width', 'height', 'bitrate_bps'):
            row[2].pop(key)
        self.assertEqual(evaluate_drama([row])['decision'], 'accept')

    def test_single_video_uses_itself_without_an_outlier(self):
        result = evaluate_drama([job(1)])
        self.assertEqual((result['decision'], result['grade'], result['method']),
                         ('accept', 2, 'stable_median'))
        self.assertEqual(result['ignored_low_count'], 0)

    def test_resolution_tolerance_and_bitrate_are_separate_conditions(self):
        # The exact 10% boundary remains in the near-720 bucket.
        self.assertEqual(evaluate_drama([job(1, 648, 1152, 1_200_000)])['grade'], 2)
        # High bitrate cannot promote a short side below the resolution boundary.
        self.assertEqual(evaluate_drama([job(1, 647, 1150, 5_000_000)])['grade'], 1)
        self.assertEqual(evaluate_drama([job(1, 1080, 1920, 1_000_000)])['grade'], 0)
        self.assertEqual(evaluate_drama([job(1, 1080, 1920, 3_375_000)])['grade'], 3)

    def test_stable_drama_uses_median_and_keeps_every_member(self):
        rows = [job(1, bitrate=1_100_000), job(2, bitrate=1_300_000),
                job(3, bitrate=1_600_000)]
        result = evaluate_drama(rows)
        self.assertEqual(result['method'], 'stable_median')
        self.assertEqual(result['median_equiv720_mbps'], 1.3)
        self.assertEqual(result['grade'], 2)
        self.assertEqual(result['expected_message_ids'], [1, 2, 3])
        self.assertEqual(len(rows), 3)

    def test_exact_stability_width_height_and_rate_boundaries(self):
        rows = [job(1, 648, 1152, 1_000_000), job(2, 720, 1280, 1_500_000)]
        self.assertTrue(evaluate_drama(rows)['stable'])
        rows[0][2]['width'] = 647
        self.assertFalse(evaluate_drama(rows)['stable'])

    def test_ten_percent_low_tail_is_ignored_only_for_evaluation(self):
        rows = [job(i) for i in range(1, 10)] + [job(10, 480, 854, 600_000)]
        result = evaluate_drama(rows)
        self.assertEqual((result['decision'], result['grade'], result['method']),
                         ('accept', 2, 'dominant_within_outlier_budget'))
        self.assertEqual(result['ignored_low_count'], 1)
        self.assertEqual(result['video_count'], 10)
        self.assertEqual(len(result['expected_video_ids']), 10)
        self.assertEqual(rows[-1][2]['width'], 480)

    def test_one_bad_in_five_is_twenty_percent_and_forces_fallback(self):
        result = evaluate_drama([job(i) for i in range(1, 5)] + [job(5, 480, 854, 600_000)])
        self.assertEqual((result['decision'], result['grade'], result['method']),
                         ('reject', 1, 'minimum_resolution_fallback'))
        self.assertEqual(result['ignored_low_count'], 0)

    def test_six_of_ten_and_one_low_is_a_valid_majority(self):
        rows = ([job(i) for i in range(1, 7)]
                + [job(i, 1080, 1920, 4_000_000) for i in range(7, 10)]
                + [job(10, 480, 854, 600_000)])
        result = evaluate_drama(rows)
        self.assertEqual((result['grade'], result['method']), (2, 'dominant_within_outlier_budget'))
        self.assertEqual(result['mode_video_count'], 6)

    def test_thirty_percent_low_quality_is_not_discarded(self):
        rows = ([job(i, 1080, 1920, 4_000_000) for i in range(1, 15)]
                + [job(i, 480, 854, 600_000) for i in range(15, 21)])
        result = evaluate_drama(rows)
        self.assertEqual((result['grade'], result['method']), (1, 'minimum_resolution_fallback'))
        self.assertEqual(result['decision'], 'reject')

    def test_tied_modes_fall_back_to_lowest_resolution_and_its_bitrate(self):
        rows = ([job(i, 1080, 1920, 4_000_000) for i in range(1, 6)]
                + [job(i, 720, 1280, 1_300_000) for i in range(6, 11)])
        result = evaluate_drama(rows)
        self.assertEqual(result['mode_grade'], None)
        self.assertEqual((result['grade'], result['method']), (2, 'minimum_resolution_fallback'))
        self.assertEqual(result['lowest_resolution_median_equiv720_mbps'], 1.3)

    def test_lowest_resolution_group_uses_its_own_median_bitrate(self):
        rows = ([job(i, 1080, 1920, 5_000_000) for i in range(1, 6)]
                + [job(6, 720, 1280, 700_000), job(7, 720, 1280, 900_000)])
        result = evaluate_drama(rows)
        self.assertEqual(result['method'], 'minimum_resolution_fallback')
        self.assertEqual(result['lowest_resolution_median_equiv720_mbps'], .8)
        self.assertEqual(result['grade'], 1)

    def test_order_is_deterministic_and_results_are_json_serializable(self):
        rows = [job(10), job(2), job(1)]
        forward, reverse = evaluate_drama(rows), evaluate_drama(reversed(rows))
        self.assertEqual(forward, reverse)
        self.assertEqual(forward['expected_message_ids'], [1, 2, 10])
        json.dumps(forward)

    def test_mixed_or_duplicate_identities_are_rejected(self):
        with self.assertRaisesRegex(PipelineError, 'mixed_drama_jobs'):
            evaluate_drama([job(1), job(2, did='other')])
        with self.assertRaisesRegex(PipelineError, 'duplicate_drama_video'):
            evaluate_drama([job(1), job(1)])


if __name__ == '__main__':
    unittest.main()
