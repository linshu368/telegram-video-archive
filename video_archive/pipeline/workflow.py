"""Connect the existing source, delivery and enrichment stages without duplicating them."""
import asyncio
import fcntl
from contextlib import contextmanager, nullcontext

from video_archive.core import Store, atomic_json, now
from video_archive.pipeline.common import PipelineError, ai_output, error_code


@contextmanager
def source_session_lock():
    """Do not share the Telegram session with another scanner or downloader."""
    from video_archive.telegram import archive
    archive.private_init()
    with (archive.PRIVATE / 'process.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PipelineError('archive_worker_running') from None
        yield


def scan_source(root, *, lock_held=False):
    """Use the metadata-only scanner and its original session lock."""
    from video_archive.telegram import archive
    args = archive.parser().parse_args(['--data', str(root), 'scan'])
    with nullcontext() if lock_held else source_session_lock():
        store = Store(root)
        try:
            code = asyncio.run(archive.execute(args, store))
            if code != 0:
                raise PipelineError('metadata_scan_incomplete')
        finally:
            try:
                store.export()
            finally:
                store.db.close()


def unprocessed_selection(path, project_ref, rows, *, only=(), first=()):
    """Keep explicit selection scoped even when every requested drama is in the pool."""
    from video_archive.pipeline.processed import filter_unprocessed_rows
    rows = list(rows)
    by_message = {job[2]['message_id']: job for job in rows}
    if any(mid not in by_message for mid in (*only, *first)):
        raise PipelineError('requested_id_not_in_inventory')
    if only:
        requested = {by_message[mid][1]['drama_id'] for mid in only}
        rows = [job for job in rows if job[1]['drama_id'] in requested]
    remaining, skipped = filter_unprocessed_rows(path, project_ref, rows)
    available = {job[2]['message_id'] for job in remaining}
    return (remaining, skipped, [mid for mid in only if mid in available],
            [mid for mid in first if mid in available])


def _groups(rows):
    groups = {}
    for job in rows:
        groups.setdefault(job[1]['drama_id'], []).append(job)
    for members in groups.values():
        members.sort(key=lambda job: job[2]['message_id'])
    return groups


def ai_committed(cloud, did, allowed_sources=None):
    """A succeeded task alone is insufficient: require the committed result too."""
    if (not cloud or str(cloud.get('drama_id')) != str(did)
            or (cloud.get('tasks', {}).get('ai') or {}).get('status') != 'succeeded'):
        return False
    result = cloud.get('ai')
    if not isinstance(result, dict) or str(result.get('drama_id')) != str(did):
        return False
    if (allowed_sources is not None
            and str(result.get('source_video_id')) not in allowed_sources):
        return False
    ai_output({key: result.get(key) for key in ('title', 'tags', 'description')})
    return True


def process_ai(db, storage, cache, rows, *, analyze, provider=None, limit=None, report_path=None):
    """Reuse the existing drama AI transaction; resume after success without paying twice."""
    from video_archive.pipeline.batch import select_ai_drama_candidates
    from video_archive.pipeline.runner import committed_inventory, verify_identity
    rows = list(rows)
    groups = _groups(rows)
    committed, _ = committed_inventory(db, rows)
    candidates = select_ai_drama_candidates(rows, committed, limit=limit)
    report = {'at': now(), 'status': 'in_progress', 'results': [], 'processed': 0}

    def save():
        report['processed'] = len(report['results'])
        if report_path:
            atomic_json(report_path, report)

    save()
    try:
        for did in candidates:
            entry = {'drama_id': did}
            allowed_sources = {str(job[1]['id']) for job in groups[did]
                               if job[2]['message_id'] in committed}
            sample = next(job for job in groups[did] if job[2]['message_id'] in committed)
            try:
                cloud = db.state(sample[1]['id'])
                verify_identity(sample[1], sample[2], cloud)
                if ai_committed(cloud, did, allowed_sources):
                    entry['result'] = 'ai_already_succeeded'
                else:
                    if provider is None:
                        from video_archive.pipeline.ai import build_provider
                        provider = build_provider()
                    analyze(db, storage, cache, did, provider, allowed_source_ids=allowed_sources)
                    cloud = db.state(sample[1]['id'])
                    verify_identity(sample[1], sample[2], cloud)
                    if not ai_committed(cloud, did, allowed_sources):
                        raise PipelineError('ai_commit_not_verified')
                    entry['result'] = 'ai_succeeded'
            except PipelineError as exc:
                entry.update(result='pending' if str(exc) == 'task_not_claimable' else 'failed',
                             error=error_code(exc))
            except Exception as exc:
                entry.update(result='failed', error=error_code(exc))
            if entry['result'] in ('pending', 'failed'):
                # A response can be lost after the transaction has already committed.
                try:
                    cloud = db.state(sample[1]['id'])
                    verify_identity(sample[1], sample[2], cloud)
                    if ai_committed(cloud, did, allowed_sources):
                        entry.update(result='ai_already_succeeded')
                        entry.pop('error', None)
                except Exception:
                    pass
            report['results'].append(entry)
            save()
        report['status'] = ('pending' if any(e['result'] not in (
            'ai_succeeded', 'ai_already_succeeded') for e in report['results']) else 'complete')
    except BaseException:
        report['status'] = 'interrupted'
        raise
    finally:
        save()
    return report


def finalize_processed(db, rows, *, path, project_ref, report_path=None):
    """Seal only dramas whose accepted videos, covers, AI and faststart are committed."""
    from video_archive.pipeline.processed import record_success
    from video_archive.pipeline.runner import committed_inventory
    rows = list(rows)
    committed, covered = committed_inventory(db, rows)
    results = []
    for did, members in _groups(rows).items():
        entry = {'drama_id': did, 'result': 'pending'}
        try:
            available = [job for job in members if job[2]['message_id'] in committed]
            if not available:
                entry['reason'] = 'no_committed_video'
            else:
                states = [db.state(job[1]['id']) for job in available]
                from video_archive.pipeline.runner import verify_identity
                for job, state in zip(available, states):
                    verify_identity(job[1], job[2], state)
                allowed_sources = {str(job[1]['id']) for job in available}
                if not all(ai_committed(state, did, allowed_sources) for state in states):
                    entry['reason'] = 'ai_pending'
                elif any(job[2]['message_id'] not in covered for job in available):
                    entry['reason'] = 'cover_pending'
                elif not all('transcode' not in state.get('tasks', {})
                             or (state['tasks']['transcode'] or {}).get('status') == 'succeeded'
                             for state in states):
                    entry['reason'] = 'transcode_pending'
                else:
                    record_success(path, project_ref, members, report_path=report_path,
                                   details={'verified_video_count': len(available),
                                            'missing_video_count': len(members) - len(available),
                                            'delivery_accepted': True, 'ai_complete': True,
                                            'transcode_complete': True, 'cover_complete': True})
                    entry['result'] = 'completed'
        except Exception as exc:
            entry['reason'] = error_code(exc)
        results.append(entry)
    return results
