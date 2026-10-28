"""Project-scoped terminal drama decisions, reopened only by explicit request."""
import json
import re
from pathlib import Path

from video_archive.core import atomic_json, now
from video_archive.pipeline.common import PipelineError
from video_archive.pipeline.completion import manifest_digest, manifest_for_jobs, manifest_map


TERMINAL_STATUSES = {'screening_rejected', 'download_rejected', 'completed'}
COMPLETION_FLAGS = ('delivery_accepted', 'ai_complete', 'transcode_complete', 'cover_complete')
DETAIL_FIELDS = (*COMPLETION_FLAGS, 'delivery_decision', 'delivery_finalized', 'expected_count',
                 'verified_video_count', 'missing_video_count', 'missing_video_ratio',
                 'planning_status', 'planning_reason', 'grade', 'grade_label', 'method')


def _project(project_ref):
    if not isinstance(project_ref, str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', project_ref):
        raise PipelineError('processed_pool_project_invalid')


def _path(path):
    path = Path(path)
    if (path.is_symlink() or path.parent.is_symlink()
            or path.with_suffix(path.suffix + '.tmp').is_symlink()):
        raise PipelineError('processed_pool_symlink')
    return path


def processed_file(root, project_ref):
    _project(project_ref)
    root = Path(root)
    if root.is_symlink():
        raise PipelineError('processed_pool_symlink')
    root = root.resolve()
    path = root / 'pipeline' / 'projects' / project_ref / 'processed-dramas.json'
    for parent in (root / 'pipeline', root / 'pipeline/projects', path.parent):
        if parent.is_symlink():
            raise PipelineError('processed_pool_symlink')
    return _path(path)


def _identity(manifest, drama_id=None):
    members = manifest_map(manifest)
    if not members:
        raise PipelineError('processed_pool_identity_incomplete')
    identities = {(member['source_id'], member.get('topic_id'), member.get('drama_id'))
                  for member in members.values()}
    if len(identities) != 1:
        raise PipelineError('processed_pool_identity_conflict')
    source, topic, did = next(iter(identities))
    if not topic or not did:
        raise PipelineError('processed_pool_identity_incomplete')
    if drama_id is not None and str(drama_id) != did:
        raise PipelineError('processed_pool_identity_conflict')
    return {'source_id': source, 'topic_id': topic, 'drama_id': did}, members


def load_pool(path, project_ref):
    _project(project_ref)
    path = _path(path)
    if not path.exists():
        return {'version': 1, 'project_ref': project_ref, 'dramas': {}}
    try:
        data = json.loads(path.read_text())
    except (ValueError, OSError):
        raise PipelineError('processed_pool_invalid') from None
    if (not isinstance(data, dict) or data.get('version') != 1
            or not isinstance(data.get('dramas'), dict)):
        raise PipelineError('processed_pool_invalid')
    if data.get('project_ref') != project_ref:
        raise PipelineError('processed_pool_project_mismatch')
    topics = set()
    for did, entry in data['dramas'].items():
        if (not isinstance(entry, dict) or entry.get('drama_id') != did
                or type(entry.get('terminal')) is not bool
                or entry.get('status') not in TERMINAL_STATUSES | {'reopened'}
                or entry['terminal'] != (entry['status'] in TERMINAL_STATUSES)
                or not isinstance(entry.get('source_id'), str) or not entry.get('source_id')
                or not isinstance(entry.get('topic_id'), str) or not entry.get('topic_id')
                or not isinstance(entry.get('reason'), str) or not entry.get('reason')
                or not isinstance(entry.get('marked_at'), str) or not entry.get('marked_at')
                or type(entry.get('expected_count')) is not int or entry['expected_count'] <= 0
                or not isinstance(entry.get('expected_message_ids'), list)
                or any(type(mid) is not int or mid <= 0 for mid in entry['expected_message_ids'])
                or len(entry['expected_message_ids']) != entry['expected_count']
                or len(set(entry['expected_message_ids'])) != entry['expected_count']
                or not re.fullmatch(r'[a-f0-9]{64}', str(entry.get('manifest_digest', '')))
                or not isinstance(entry.get('history'), list)):
            raise PipelineError('processed_pool_invalid')
        key = entry['source_id'], entry['topic_id']
        if key in topics:
            raise PipelineError('processed_pool_identity_conflict')
        topics.add(key)
        if entry['status'] == 'completed' and not _success_confirmed(entry.get('details')):
            raise PipelineError('processed_pool_success_unconfirmed')
    return data


def _groups(rows):
    rows, grouped = list(rows), {}
    for row in rows:
        grouped.setdefault(row[1]['drama_id'], []).append(row)
    result = {}
    for did, members in grouped.items():
        manifest = manifest_for_jobs(members)
        identity, _ = _identity(manifest, did)
        result[did] = (members, manifest, identity)
    return rows, result


def _existing(data, identity):
    entry = data['dramas'].get(identity['drama_id'])
    if entry is not None:
        if any(entry.get(key) != value for key, value in identity.items()):
            raise PipelineError('processed_pool_identity_conflict')
        return entry
    if any(entry['source_id'] == identity['source_id'] and entry['topic_id'] == identity['topic_id']
           for entry in data['dramas'].values()):
        raise PipelineError('processed_pool_identity_conflict')
    return None


def filter_unprocessed_rows(path, project_ref, rows):
    """Skip the whole terminal source topic even when its video manifest grows."""
    data = load_pool(path, project_ref)
    rows, groups = _groups(rows)
    skipped, skipped_ids = [], set()
    for did, (members, manifest, identity) in groups.items():
        entry = _existing(data, identity)
        if not entry or not entry['terminal']:
            continue
        skipped_ids.add(did)
        skipped.append({**identity, 'status': entry['status'], 'reason': entry['reason'],
                        'video_count': len(members), 'recorded_video_count': entry['expected_count'],
                        'marked_at': entry['marked_at'], 'report_path': entry.get('report_path'),
                        'manifest_changed': entry['manifest_digest'] != manifest_digest(manifest)})
    return [row for row in rows if row[1]['drama_id'] not in skipped_ids], skipped


def _save(path, data):
    path = _path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _path(path)
    data['updated_at'] = now()
    atomic_json(path, data)


def _mark(data, manifest, status, reason, report_path, details):
    identity, members = _identity(manifest)
    previous = _existing(data, identity)
    if previous and previous['terminal']:
        return previous, False
    stamp = now()
    detail = {key: details[key] for key in DETAIL_FIELDS if key in details}
    event = {'action': 'marked', 'status': status, 'reason': reason, 'at': stamp,
             'manifest_digest': manifest_digest(manifest),
             'report_path': str(report_path) if report_path is not None else None}
    entry = {**identity, 'status': status, 'terminal': True, 'reason': reason,
             'marked_at': stamp, 'updated_at': stamp, 'expected_count': len(members),
             'expected_message_ids': sorted(members), 'manifest_digest': event['manifest_digest'],
             'report_path': event['report_path'], 'details': detail,
             'history': [*(previous.get('history', []) if previous else []), event]}
    data['dramas'][identity['drama_id']] = entry
    return entry, True


def record_evaluations(path, project_ref, evaluations, report_path=None):
    """Seal explicit quality rejection or a finalized download rejection only."""
    data, marked, changed = load_pool(path, project_ref), [], False
    for evaluation in evaluations:
        if (evaluation.get('decision') == 'reject'
                and evaluation.get('reason') in ('non_portrait_video', 'below_minimum_quality')):
            status, reason = 'screening_rejected', evaluation['reason']
        elif (evaluation.get('decision') == 'accept'
              and evaluation.get('delivery_finalized') is True
              and evaluation.get('delivery_decision') == 'rejected_incomplete'
              and evaluation.get('delivery_accepted') is False
              and not evaluation.get('delivery_stopped') and not evaluation.get('delivery_interrupted')):
            status, reason = 'download_rejected', 'delivery_missing_ratio_exceeded'
        else:
            continue
        manifest = evaluation.get('expected_manifest')
        if manifest is None:
            continue  # A legacy/partial decision cannot establish a whole-drama terminal identity.
        _, members = _identity(manifest, evaluation.get('drama_id'))
        expected = evaluation.get('expected_message_ids')
        if expected is not None and (not isinstance(expected, list)
                or any(type(mid) is not int or mid <= 0 for mid in expected)
                or len(expected) != len(set(expected)) or set(expected) != set(members)):
            raise PipelineError('processed_pool_manifest_conflict')
        entry, updated = _mark(data, manifest, status, reason,
                               report_path if report_path is not None else evaluation.get('report_path'), evaluation)
        marked.append(entry)
        changed = changed or updated
    if changed:
        _save(path, data)
    return marked


def _success_confirmed(details):
    return isinstance(details, dict) and all(details.get(key) is True for key in COMPLETION_FLAGS)


def record_success(path, project_ref, rows, *, report_path=None, details=None):
    """Seal one complete source drama after the caller verifies every final stage.

    Delivery acceptance alone is insufficient. AI, transcode and cover completion
    must each be explicitly confirmed by the caller; pending stages stay reusable.
    """
    data = load_pool(path, project_ref)
    if not _success_confirmed(details):
        return None
    _, groups = _groups(rows)
    if len(groups) != 1:
        raise PipelineError('processed_success_requires_one_drama')
    members, manifest, _ = next(iter(groups.values()))
    if 'expected_count' in details and (type(details['expected_count']) is not int
                                         or details['expected_count'] != len(members)):
        raise PipelineError('processed_pool_manifest_conflict')
    if 'verified_video_count' in details and 'missing_video_count' in details:
        counts = [details['verified_video_count'], details['missing_video_count']]
        if any(type(count) is not int or count < 0 for count in counts) or sum(counts) != len(members):
            raise PipelineError('processed_pool_manifest_conflict')
    entry, changed = _mark(data, manifest, 'completed', 'pipeline_completed', report_path, details)
    if changed:
        _save(path, data)
    return entry


def reopen(path, project_ref, drama_ids):
    """Explicitly reopen IDs (or all with None), retaining their terminal history."""
    data = load_pool(path, project_ref)
    ids = list(data['dramas']) if drama_ids is None else list(dict.fromkeys(
        [drama_ids] if isinstance(drama_ids, str) else drama_ids))
    stamp, reopened = now(), []
    for did in ids:
        entry = data['dramas'].get(did)
        if not entry or not entry['terminal']:
            continue
        entry.update(previous_status=entry['status'], status='reopened', terminal=False,
                     reopened_at=stamp, updated_at=stamp)
        entry['history'].append({'action': 'reopened', 'at': stamp, 'reason': 'manual_reopen',
                                 'previous_status': entry['previous_status']})
        reopened.append(entry)
    if reopened:
        _save(path, data)
    return reopened
