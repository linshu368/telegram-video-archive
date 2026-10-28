"""Source manifests and durable completion evidence; no media or network IO."""
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

from video_archive.pipeline.common import PipelineError


IDENTITY_FIELDS = ('source_id', 'message_id', 'media_id', 'video_id', 'expected_size')


def _integer(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise PipelineError('source_manifest_invalid')
    try:
        result = int(value)
    except ValueError:
        raise PipelineError('source_manifest_invalid') from None
    if not result or (positive and result < 0):
        raise PipelineError('source_manifest_invalid')
    return result


def normalize_member(member):
    if not isinstance(member, dict):
        raise PipelineError('source_manifest_invalid')
    try:
        source = _integer(member['source_id'])
        mid = _integer(member['message_id'], positive=True)
        media = _integer(member['media_id'], positive=True)
        vid = member['video_id']
    except KeyError:
        raise PipelineError('source_manifest_invalid') from None
    if not isinstance(vid, str) or not vid:
        raise PipelineError('source_manifest_invalid')
    size, topic = member.get('expected_size'), member.get('topic_id')
    return {'source_id': str(source), 'message_id': mid, 'media_id': str(media),
            'video_id': vid, 'expected_size': None if size is None else _integer(size, positive=True),
            'topic_id': None if topic is None else str(_integer(topic, positive=True)),
            'drama_id': str(member['drama_id']) if member.get('drama_id') is not None else None}


def manifest_map(manifest):
    if not isinstance(manifest, list):
        raise PipelineError('source_manifest_invalid')
    result, video_ids = {}, set()
    for raw in manifest:
        member = normalize_member(raw)
        mid = member['message_id']
        if mid in result or member['video_id'] in video_ids:
            raise PipelineError('source_manifest_duplicate_identity')
        result[mid] = member
        video_ids.add(member['video_id'])
    return result


def manifest_for_jobs(jobs):
    members = []
    for drama, video, record, _ in jobs:
        members.append({'source_id': record['source']['id'], 'message_id': record['message_id'],
                        'media_id': record['media_id'], 'video_id': video['id'],
                        'drama_id': video['drama_id'],
                        'topic_id': record.get('topic_id', drama.get('topic_id')),
                        'expected_size': record.get('expected_size')})
    by_message = manifest_map(members)
    return [by_message[mid] for mid in sorted(by_message)]


def manifest_digest(manifest):
    normalized = manifest_map(manifest)
    body = json.dumps([normalized[mid] for mid in sorted(normalized)],
                      sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(body).hexdigest()


def check_result_identity(entry, expected):
    """Reject contradictory evidence; label absent legacy fields without filling them."""
    if not isinstance(entry, dict):
        raise PipelineError('completion_result_invalid')
    for field in (*IDENTITY_FIELDS, 'drama_id', 'topic_id'):
        if field not in entry:
            continue
        actual, wanted = entry[field], expected.get(field)
        if field == 'expected_size' and actual is None and wanted is None:
            continue
        if field in ('message_id', 'expected_size'):
            if type(actual) is not int or actual != wanted:
                raise PipelineError('completion_result_identity_conflict')
        elif actual is None or str(actual) != str(wanted):
            if actual is not None or wanted is not None:
                raise PipelineError('completion_result_identity_conflict')
    return [field for field in IDENTITY_FIELDS if field not in entry]


def check_segment(part, manifest, allowed_ids, project_ref, *, require_project=False):
    if not isinstance(part, dict) or not isinstance(part.get('videos'), list):
        raise PipelineError('completion_segment_invalid')
    if ('project_ref' in part and part['project_ref'] != project_ref
            or require_project and 'project_ref' not in part):
        raise PipelineError('completion_segment_project_mismatch')
    if 'expected_manifest' in part:
        declared = manifest_map(part['expected_manifest'])
        if set(declared) != set(allowed_ids):
            raise PipelineError('completion_segment_manifest_conflict')
        for mid, member in declared.items():
            if mid in manifest:
                check_result_identity(member, manifest[mid])
    seen = set()
    missing = {}
    for entry in part['videos']:
        mid = entry.get('message_id') if isinstance(entry, dict) else None
        if type(mid) is not int or mid not in allowed_ids:
            raise PipelineError('completion_result_unexpected_id')
        if mid in seen:
            raise PipelineError('completion_result_duplicate_id')
        seen.add(mid)
        missing[mid] = (check_result_identity(entry, manifest[mid]) if mid in manifest
                        else list(IDENTITY_FIELDS))
    return missing


def _message_ids(evaluation, name):
    values = evaluation.get(name, [])
    if (not isinstance(values, list) or any(type(mid) is not int or mid <= 0 for mid in values)
            or len(values) != len(set(values))):
        raise PipelineError('source_manifest_duplicate_identity')
    return set(values)


def _delivery(evaluation, expected, video_done, latest, identity_known):
    """Judge finalized video delivery, independently of outstanding covers."""
    try:
        ratio = Decimal(str(evaluation.get('max_missing_ratio', .10)))
        if not ratio.is_finite() or not 0 <= ratio <= 1:
            raise ValueError()
    except (InvalidOperation, ValueError, TypeError):
        raise PipelineError('invalid_completion_policy') from None
    missing = expected - video_done
    result = {'delivery_decision': 'pending', 'delivery_accepted': False,
              'max_missing_ratio': float(ratio),
              'missing_video_ratio': float(Decimal(len(missing)) / len(expected)) if expected else None}
    if (evaluation.get('delivery_finalized') is not True or not expected or not identity_known
            or evaluation.get('delivery_stopped') or evaluation.get('delivery_interrupted')
            or evaluation.get('decision') != 'accept'
            or evaluation.get('planning_status') not in ('scheduled', 'already_complete')):
        return result
    # Queued work, a live lease and a backoff are still work, even if their share
    # is small. Only a real failure or an authoritative exhausted-attempts result
    # can be counted as a finalized gap.
    for mid in missing:
        entry = latest.get(mid, {})
        if entry.get('error') in ('lease_active', 'database_backoff', 'task_not_claimable'):
            return result
        if not (entry.get('result') == 'failed'
                or entry.get('result') == 'retry_deferred' and entry.get('error') == 'attempts_exhausted'):
            return result
    if not missing:
        decision = 'complete'
    elif Decimal(len(missing)) < ratio * len(expected):
        decision = 'accepted_with_gaps'
    else:
        decision = 'rejected_incomplete'
    result.update(delivery_decision=decision,
                  delivery_accepted=decision in ('complete', 'accepted_with_gaps'))
    return result


def drama_progress(evaluation, segments, video_success, cover_success, active_drama_id=None):
    """Derive exact member completion and retained attempt history from receipts."""
    expected = _message_ids(evaluation, 'expected_message_ids')
    scheduled = _message_ids(evaluation, 'scheduled_message_ids')
    committed = _message_ids(evaluation, 'committed_message_ids')
    complete = _message_ids(evaluation, 'complete_message_ids')
    cover_only = _message_ids(evaluation, 'cover_only_message_ids')
    if not scheduled <= expected or not complete <= committed <= expected or not cover_only <= scheduled:
        raise PipelineError('drama_selection_identity_conflict')
    manifest = (manifest_map(evaluation['expected_manifest'])
                if 'expected_manifest' in evaluation else {})
    if 'expected_manifest' in evaluation and set(manifest) != expected:
        raise PipelineError('drama_selection_identity_conflict')
    latest, latest_identity, histories, legacy = {}, {}, {mid: [] for mid in expected}, False
    for number, segment in enumerate(segments, 1):
        seen = set()
        for entry in segment.get('videos', []):
            mid = entry['message_id']
            if mid not in expected:
                continue
            if mid in seen:
                raise PipelineError('completion_result_duplicate_id')
            seen.add(mid)
            missing = (check_result_identity(entry, manifest[mid]) if mid in manifest
                       else list(IDENTITY_FIELDS))
            legacy = legacy or bool(missing)
            attempt = {'segment_number': number, 'retry_round': segment.get('retry_round', 0),
                       'result': entry.get('result'), 'cover_result': entry.get('cover_result'),
                       'identity_evidence': 'legacy_result_fields_missing' if missing else 'matched_manifest',
                       'missing_identity_fields': missing}
            attempt.update({key: entry[key] for key in ('error', 'cover_error') if key in entry})
            histories[mid].append(attempt)
            latest[mid] = entry
            latest_identity[mid] = not missing
    video_done = committed - (scheduled - cover_only)
    cover_done = complete - scheduled
    video_done.update(mid for mid in scheduled if latest.get(mid, {}).get('result') in video_success)
    cover_done.update(mid for mid in scheduled if latest.get(mid, {}).get('cover_result') in cover_success)
    work_left = {mid for mid in scheduled if latest.get(mid, {}).get('result') not in video_success
                 or latest.get(mid, {}).get('cover_result') not in cover_success}
    if not video_done <= expected or not cover_done <= expected:
        raise PipelineError('completion_result_unexpected_id')
    remaining = expected - video_done | expected - cover_done | work_left
    video_complete, cover_complete = bool(expected) and video_done == expected, bool(expected) and cover_done == expected
    status = evaluation.get('planning_status')
    if status == 'rejected':
        state = 'rejected'
    elif status == 'pending':
        state = 'metadata_pending'
    elif status == 'deferred':
        state = 'deferred'
    elif video_complete and cover_complete and not work_left:
        state = 'complete'
    elif active_drama_id == evaluation['drama_id']:
        state = 'in_progress'
    else:
        state = 'incomplete' if any(mid in latest for mid in scheduled) else 'queued'
    episodes = []
    for mid in sorted(expected):
        entry, attempts = latest.get(mid, {}), histories[mid]
        failures = [attempt for attempt in attempts if attempt['result'] not in video_success
                    or attempt['cover_result'] not in cover_success]
        episode = {'message_id': mid, 'expected_identity': manifest.get(mid),
                   'video_status': 'verified' if mid in video_done else entry.get('result', state),
                   'cover_status': 'verified' if mid in cover_done else entry.get('cover_result', state),
                   'identity_evidence': (attempts[-1]['identity_evidence'] if attempts else
                                         ('previous_cloud_commit' if mid in video_done else 'not_received')),
                   'attempt_history': attempts, 'failure_history': failures}
        episode.update({key: entry[key] for key in ('error', 'cover_error') if key in entry})
        episodes.append(episode)
    return {'state': state, 'video_complete': video_complete, 'cover_complete': cover_complete,
            **_delivery(evaluation, expected, video_done, latest,
                        bool(manifest) and all(latest_identity.values())),
            'expected_count': len(expected), 'verified_video_count': len(video_done),
            'missing_video_count': len(expected - video_done), 'verified_cover_count': len(cover_done),
            'missing_cover_count': len(expected - cover_done),
            'verified_message_ids': sorted(video_done),
            'verified_video_ids': [manifest[mid]['video_id'] for mid in sorted(video_done) if mid in manifest],
            'remaining_message_ids': sorted(remaining),
            'remaining_video_message_ids': sorted(expected - video_done),
            'remaining_cover_message_ids': sorted(expected - cover_done), 'episodes': episodes,
            'completion_identity_status': ('legacy_manifest_missing' if not manifest else
                                             'legacy_result_identity_missing' if legacy else 'matched_manifest'),
            'failure_history': [{'message_id': episode['message_id'], **attempt}
                                for episode in episodes for attempt in episode['failure_history']]}


def validate_retry_manifests(report, rows):
    """Match each saved source manifest to the complete current drama, never its prefix."""
    groups = {}
    for row in rows:
        groups.setdefault(row[1]['drama_id'], []).append(row)
    checked, legacy = 0, False
    for drama in report.get('dramas', []):
        if 'expected_manifest' not in drama:
            legacy = True
            continue
        saved = manifest_map(drama['expected_manifest'])
        current = manifest_map(manifest_for_jobs(groups.get(drama.get('drama_id'), [])))
        if set(saved) != set(current):
            raise PipelineError('retry_source_manifest_conflict')
        for mid, member in saved.items():
            # A formerly unknown size/topic can be filled; a known identity can
            # never silently change or be replaced by another source file.
            if any(value is not None and value != current[mid].get(key)
                   for key, value in member.items()):
                raise PipelineError('retry_source_manifest_conflict')
        checked += 1
    return ('matched_source_manifest' if checked and not legacy else 'legacy_report_manifest_missing')


def recover_active_segment(report, report_path, project_ref):
    """Recover each lane's interrupted sibling report exactly once.

    The legacy single-child fields remain readable. Every child must match its
    project, source manifest, owning drama and saved set of segment members.
    """
    active = report.get('active_segment_reports', {})
    if not isinstance(active, dict):
        raise PipelineError('active_segment_report_invalid')
    descriptors = []
    for did, descriptor in active.items():
        if (not isinstance(descriptor, dict) or not descriptor.get('path')
                or str(descriptor.get('drama_id', did)) != str(did)):
            raise PipelineError('active_segment_report_drama_conflict')
        descriptors.append((str(did), {**descriptor, 'drama_id': did}, False))
    if report.get('active_segment_report'):
        descriptors.append(('legacy', {
            'path': report['active_segment_report'],
            'message_ids': report.get('active_segment_message_ids'),
            'drama_id': report.get('active_segment_drama_id'),
            'retry_round': report.get('active_segment_retry_round', 0),
        }, True))
    if not descriptors:
        return False
    parent = Path(report_path).resolve()
    expected, identities, by_drama = set(), {}, {}
    for drama in report.get('dramas', []):
        did = str(drama['drama_id'])
        if did in by_drama:
            raise PipelineError('source_manifest_duplicate_identity')
        members = _message_ids(drama, 'expected_message_ids')
        if members & expected:
            raise PipelineError('source_manifest_duplicate_identity')
        by_drama[did] = members
        expected.update(members)
        if 'expected_manifest' in drama:
            declared = manifest_map(drama['expected_manifest'])
            if set(declared) != members:
                raise PipelineError('drama_selection_identity_conflict')
            identities.update(declared)
    if not expected:
        expected.update(report.get('pending_message_ids', []))
        expected.update(entry['message_id'] for segment in report.get('segments', [])
                        for entry in segment.get('videos', []))
    recovered, seen_paths = False, set()
    statuses = report.setdefault('active_segment_recoveries', {})
    for key, descriptor, legacy in descriptors:
        path = Path(descriptor['path'])
        path = path if path.is_absolute() else parent.parent / path
        if (path.is_symlink() or path.resolve().parent != parent.parent
                or not path.name.startswith(parent.stem + '-') or path.suffix != '.json'):
            raise PipelineError('active_segment_report_path_conflict')
        path = path.resolve()
        if path in seen_paths:
            if legacy:
                continue
            raise PipelineError('active_segment_report_drama_conflict')
        seen_paths.add(path)
        did = descriptor.get('drama_id')
        owned = expected
        if did is not None:
            did = str(did)
            if did not in by_drama:
                raise PipelineError('active_segment_report_drama_conflict')
            owned = by_drama[did]
        allowed_values = descriptor.get('message_ids')
        if allowed_values is None:
            if not legacy:
                raise PipelineError('active_segment_report_invalid')
            allowed = set(owned)
        else:
            allowed = _message_ids({'message_ids': allowed_values}, 'message_ids')
        if not allowed <= owned:
            raise PipelineError('completion_result_unexpected_id')
        if not path.is_file():
            statuses[key] = 'report_file_missing'
            continue
        if any(part.get('segment_report') == str(path) for part in report.get('segments', [])):
            statuses[key] = 'already_recovered'
            continue
        try:
            part = json.loads(path.read_text())
        except (ValueError, OSError):
            raise PipelineError('active_segment_report_unreadable') from None
        if not isinstance(part, dict):
            raise PipelineError('active_segment_report_unreadable')
        if 'project_ref' not in part:
            statuses[key] = 'legacy_segment_project_missing'
            continue
        if did is not None and part.get('drama_id') is not None and str(part['drama_id']) != did:
            raise PipelineError('active_segment_report_drama_conflict')
        check_segment(part, identities, allowed, project_ref, require_project=True)
        # Older parents did not save the child path in completed segments.
        if any(previous.get('videos') == part['videos']
               and previous.get('at') == part.get('at') for previous in report.get('segments', [])):
            statuses[key] = 'already_recovered'
            continue
        part.update(segment_report=str(path), recovered_after_interruption=True)
        if did is not None:
            part['drama_id'] = did
        retry_round = descriptor.get('retry_round', 0)
        if type(retry_round) is not int or retry_round < 0:
            raise PipelineError('active_segment_report_invalid')
        if retry_round:
            part['retry_round'] = retry_round
        report.setdefault('segments', []).append(part)
        statuses[key] = 'recovered'
        recovered = True
    report['active_segment_recovery'] = ('recovered' if recovered else
                                        next(reversed(statuses.values()), 'not_recovered'))
    return recovered


def read_pass_report(path, project_ref):
    report = json.loads(Path(path).read_text())
    if report.get('project_ref') != project_ref:
        raise PipelineError('retry_report_project_mismatch')
    if report.get('status') in ('interrupted', 'in_progress', 'retry_backoff', 'stopped'):
        recover_active_segment(report, path, project_ref)
    return report
