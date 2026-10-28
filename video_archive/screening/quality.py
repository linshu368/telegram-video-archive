"""Deterministic, metadata-only decisions for a complete drama inventory.

The bitrate is an estimated average container bitrate, not a perceptual quality
score.  No file, network or database access is performed by this module.
"""
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from statistics import median

from video_archive.pipeline.common import PipelineError


INT_MAX = 2_147_483_647
POLICY_VERSION = 'drama_quality_v1'
GRADE_LABELS = {0: 'below_480', 1: 'near_480', 2: 'near_720', 3: 'near_1080'}


def _decimal(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return number if number.is_finite() else None


def _positive_int(value, maximum=None):
    number = _decimal(value)
    if number is None or number <= 0 or number != number.to_integral_value():
        return None
    if maximum is not None and number > maximum:
        return None
    return int(number)


@dataclass(frozen=True)
class QualityPolicy:
    minimum_grade: int = 2
    resolution_tolerance: float = .10
    stable_bitrate_ratio: float = 1.5
    majority_share: float = .60
    max_low_share: float = .10

    def __post_init__(self):
        if type(self.minimum_grade) is not int or self.minimum_grade not in (1, 2, 3):
            raise PipelineError('invalid_quality_policy')
        values = {name: _decimal(getattr(self, name)) for name in (
            'resolution_tolerance', 'stable_bitrate_ratio', 'majority_share', 'max_low_share')}
        if (any(value is None for value in values.values())
                or not 0 <= values['resolution_tolerance'] < 1
                or values['stable_bitrate_ratio'] < 1
                or not Decimal('.5') < values['majority_share'] <= 1
                or not 0 <= values['max_low_share'] < Decimal('.5')):
            raise PipelineError('invalid_quality_policy')

    def to_dict(self):
        return {'version': POLICY_VERSION, 'minimum_grade': self.minimum_grade,
                **{name: float(_decimal(getattr(self, name))) for name in (
                    'resolution_tolerance', 'stable_bitrate_ratio', 'majority_share', 'max_low_share')}}


def media_metadata(record, state=None):
    """Use declared fields, deriving average bitrate from size and duration.

    Existing verified local probe data can fill absent declaration fields.  A
    source-declared size takes precedence over a previous local file's size.
    Dimensions describe the source's recorded display orientation; rotation
    metadata is not inferred or silently applied here.
    """
    state = state or {}
    probe = state.get('probe') or {}
    probe = probe if isinstance(probe, dict) else {}
    streams = probe.get('video_streams') or []
    streams = streams if isinstance(streams, (list, tuple)) else []
    stream = streams[0] if streams and isinstance(streams[0], dict) else {}
    width = _positive_int(record.get('width'), INT_MAX)
    height = _positive_int(record.get('height'), INT_MAX)
    if width is None:
        width = _positive_int(stream.get('width'), INT_MAX)
    if height is None:
        height = _positive_int(stream.get('height'), INT_MAX)
    bps = _positive_int(record.get('bitrate_bps'), INT_MAX)
    if bps is None:
        size = _positive_int(record.get('expected_size')) or _positive_int(state.get('actual_size'))
        duration = _decimal(record.get('duration_seconds'))
        if duration is None or duration <= 0:
            duration = _decimal(probe.get('duration'))
        if size is not None and duration is not None and duration > 0:
            try:
                value = (Decimal(size) * 8 / duration).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
                bps = _positive_int(value, INT_MAX)
            except InvalidOperation:
                bps = None
    return {'width': width, 'height': height, 'bitrate_bps': bps}


def _resolution_grade(short_side, policy):
    tolerance = Decimal('1') - _decimal(policy.resolution_tolerance)
    for grade, side in ((3, 1080), (2, 720), (1, 480)):
        if short_side >= Decimal(side) * tolerance:
            return grade
    return 0


def _quality_grade(short_side, eq720, policy):
    resgrade = _resolution_grade(short_side, policy)
    for grade, bitrate in ((3, '1.5'), (2, '1.2'), (1, '.8')):
        if resgrade >= grade and eq720 >= Decimal(bitrate):
            return grade
    return 0


def _metadata_for_job(video, record, state):
    combined = dict(record)
    # The importer may already have added metadata to its video payload.  Never
    # overwrite a valid original source declaration with an absent DB field.
    for key in ('width', 'height', 'bitrate_bps'):
        if _positive_int(combined.get(key), INT_MAX) is None:
            value = _positive_int(video.get(key), INT_MAX)
            if value is not None:
                combined[key] = value
    return media_metadata(combined, state)


def evaluate_drama(jobs, policy=None):
    """Evaluate all jobs of one drama without removing any member.

    Returns accept/reject/pending.  Unknown metadata stays pending; a known
    non-portrait member rejects the drama even if another member is unknown.
    The low-quality allowance affects evaluation only, never download selection.
    """
    policy = QualityPolicy() if policy is None else policy
    if not isinstance(policy, QualityPolicy):
        raise PipelineError('invalid_quality_policy')
    jobs = list(jobs)
    if not jobs:
        return {'drama_id': None, 'decision': 'pending', 'reason': 'empty_drama',
                'grade': None, 'method': 'missing_metadata', 'video_count': 0,
                'expected_message_ids': [], 'expected_video_ids': [],
                'missing_metadata_count': 0, 'ignored_low_count': 0,
                'policy': policy.to_dict()}
    drama_ids, message_ids, video_ids, rows = set(), [], [], []
    for drama, video, record, state in jobs:
        did = video.get('drama_id') or drama.get('id')
        if did is None or (drama.get('id') is not None and drama['id'] != did):
            raise PipelineError('invalid_drama_identity')
        drama_ids.add(str(did))
        if record.get('message_id') is None or video.get('id') is None:
            raise PipelineError('invalid_drama_video_identity')
        message_ids.append(record['message_id'])
        video_ids.append(str(video['id']))
        rows.append(_metadata_for_job(video, record, state))
    if len(drama_ids) != 1:
        raise PipelineError('mixed_drama_jobs')
    if len(set(message_ids)) != len(jobs) or len(set(video_ids)) != len(jobs):
        raise PipelineError('duplicate_drama_video')
    n = len(jobs)
    missing = sum(any(row[key] is None for key in ('width', 'height', 'bitrate_bps')) for row in rows)
    result = {
        'drama_id': next(iter(drama_ids)), 'decision': 'pending', 'reason': 'missing_video_metadata',
        'grade': None, 'method': 'missing_metadata', 'video_count': n,
        'expected_message_ids': sorted(message_ids, key=lambda value: (str(type(value)), value)),
        'expected_video_ids': sorted(video_ids), 'missing_metadata_count': missing,
        'ignored_low_count': 0, 'policy': policy.to_dict(),
    }
    if any(row['width'] is not None and row['height'] is not None
           and row['width'] >= row['height'] for row in rows):
        result.update(decision='reject', reason='non_portrait_video', method='orientation')
        return result
    if missing:
        return result
    for row in rows:
        row['short_side'] = Decimal(min(row['width'], row['height']))
        row['eq720'] = (Decimal(row['bitrate_bps']) * Decimal(921600)
                        / Decimal(row['width'] * row['height']) / Decimal(1000000))
        row['resgrade'] = _resolution_grade(row['short_side'], policy)
        row['grade'] = _quality_grade(row['short_side'], row['eq720'], policy)
    widths, heights, rates = ([row[key] for row in rows] for key in ('width', 'height', 'bitrate_bps'))
    tolerance = _decimal(policy.resolution_tolerance)
    stable = (Decimal(max(widths) - min(widths)) / max(widths) <= tolerance
              and Decimal(max(heights) - min(heights)) / max(heights) <= tolerance
              and Decimal(max(rates)) / min(rates) <= _decimal(policy.stable_bitrate_ratio))
    med_side = median(row['short_side'] for row in rows)
    med_rate = median(row['eq720'] for row in rows)
    counts = Counter(row['grade'] for row in rows)
    result.update(median_short_side=float(med_side), median_equiv720_mbps=float(med_rate),
                  grade_counts={str(grade): counts[grade] for grade in sorted(counts)},
                  stable=stable)
    if stable:
        grade = _quality_grade(med_side, med_rate, policy)
        method = 'stable_median'
    else:
        top_count = max(counts.values())
        modes = [grade for grade, count in counts.items() if count == top_count]
        mode_grade = modes[0] if len(modes) == 1 else None
        low_count = sum(count for grade, count in counts.items()
                        if mode_grade is not None and grade < mode_grade)
        result.update(mode_grade=mode_grade, mode_video_count=top_count,
                      low_video_count=low_count if mode_grade is not None else None)
        if (mode_grade is not None
                and Decimal(top_count) >= Decimal(n) * _decimal(policy.majority_share)
                and Decimal(low_count) <= Decimal(n) * _decimal(policy.max_low_share)):
            grade, method = mode_grade, 'dominant_within_outlier_budget'
            result['ignored_low_count'] = low_count
        else:
            lowest_res = min(row['resgrade'] for row in rows)
            low_rate = median(row['eq720'] for row in rows if row['resgrade'] == lowest_res)
            # Use a canonical boundary for the lowest resolution bucket, then
            # judge its own typical bitrate. Higher-resolution members cannot
            # lift this fallback's resolution.
            boundary = {0: Decimal(0), 1: Decimal(480), 2: Decimal(720), 3: Decimal(1080)}[lowest_res]
            grade, method = _quality_grade(boundary, low_rate, policy), 'minimum_resolution_fallback'
            result.update(lowest_resolution_grade=lowest_res,
                          lowest_resolution_median_equiv720_mbps=float(low_rate))
    result.update(grade=grade, grade_label=GRADE_LABELS[grade], method=method,
                  decision='accept' if grade >= policy.minimum_grade else 'reject',
                  reason='quality_accepted' if grade >= policy.minimum_grade else 'below_minimum_quality')
    return result
