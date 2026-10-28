"""Persist drama delivery decisions and gate later work by the same source manifest."""
import json
from pathlib import Path

from video_archive.core import atomic_json, now
from video_archive.pipeline.common import PipelineError
from video_archive.pipeline.completion import manifest_digest, manifest_for_jobs, manifest_map


def outcome_file(root, project_ref):
    return Path(root) / 'pipeline' / 'projects' / project_ref / 'drama-outcomes.json'


def _load(path, project_ref):
    path = Path(path)
    if path.is_symlink():
        raise PipelineError('drama_outcomes_symlink')
    if not path.exists():
        return {'version': 1, 'project_ref': project_ref, 'dramas': {}}
    data = json.loads(path.read_text())
    if (not isinstance(data, dict) or data.get('version') != 1
            or not isinstance(data.get('dramas'), dict)):
        raise PipelineError('drama_outcomes_invalid')
    if data.get('project_ref') != project_ref:
        raise PipelineError('drama_outcomes_project_mismatch')
    return data


def write_outcomes(path, project_ref, evaluations):
    """Update current decisions; timestamped pass reports retain full attempt history."""
    path = Path(path)
    data = _load(path, project_ref)
    stamp = now()
    for evaluation in evaluations:
        did = evaluation['drama_id']
        manifest = evaluation.get('expected_manifest')
        # Old reports can be inspected, but cannot establish a new delivery gate.
        if manifest is None:
            data['dramas'][did] = {'drama_id': did, 'state': 'pending',
                                   'delivery_accepted': False, 'delivery_finalized': False,
                                   'delivery_decision': 'pending', 'updated_at': stamp}
            continue
        members = manifest_map(manifest)
        accepted = (evaluation.get('delivery_accepted') is True
                    and evaluation.get('delivery_finalized') is True
                    and evaluation.get('delivery_decision') in ('complete', 'accepted_with_gaps'))
        if accepted and evaluation.get('decision') != 'accept':
            raise PipelineError('drama_outcomes_quality_conflict')
        entry = {key: evaluation[key] for key in (
            'state', 'delivery_decision', 'delivery_finalized', 'missing_video_count',
            'verified_video_count', 'missing_video_ratio', 'cover_complete', 'report_path',
            'planning_status', 'planning_reason', 'remaining_video_message_ids',
            'remaining_cover_message_ids', 'failure_history') if key in evaluation}
        entry.update(drama_id=did, manifest_digest=manifest_digest(manifest),
                     expected_count=len(members), expected_message_ids=sorted(members),
                     delivery_accepted=accepted, updated_at=stamp)
        data['dramas'][did] = entry
    data['updated_at'] = stamp
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    atomic_json(path, data)


def filter_approved_rows(path, project_ref, rows):
    """Unknown, unfinished, rejected, or changed dramas stay out of later stages."""
    data = _load(path, project_ref)
    rows = list(rows)
    groups = {}
    for job in rows:
        groups.setdefault(job[1]['drama_id'], []).append(job)
    allowed = set()
    for did, members in groups.items():
        entry = data['dramas'].get(did) or {}
        if (entry.get('delivery_accepted') is True and entry.get('delivery_finalized') is True
                and entry.get('delivery_decision') in ('complete', 'accepted_with_gaps')
                and entry.get('manifest_digest') == manifest_digest(manifest_for_jobs(members))):
            allowed.add(did)
    return [job for job in rows if job[1]['drama_id'] in allowed]
