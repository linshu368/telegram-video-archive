"""Bounded download -> upload batch. Explicit --limit only; not a bulk worker."""
import asyncio
import json
import signal
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import archive
from core import StopArchive, atomic_json, filename, now
from pipeline import (archive_existing, cover_download_task, cover_upload_task,
                      download_task, sample_evidence, transcode_task)
from pipeline_common import PipelineError, error_code, mark_cover_remote

MAX_BATCH = 100
# 'video_already_committed': this pass carried the video only to reach its cover,
# so not re-verifying the object is the expected outcome, not an incomplete one.
SUCCESS = {'uploaded_readback_verified', 'already_archived_readback_verified',
           'video_already_committed'}
COVER_SUCCESS = {'cover_uploaded_readback_verified', 'already_archived', 'no_thumbnail', 'skipped'}


def plan(rows, root, archived, *, only=(), first=(), max_bytes=128 * 1024**2,
         bucket_limit=None, defer_ids=(3021,), resume_partials=False, covered=frozenset(),
         cover_only=frozenset()):
    """Select once for a finite pass. Failures never monopolize later segments."""
    known = {r['message_id']: job for job in rows for r in [job[2]]}
    if any(mid not in known for mid in (*only, *first)):
        raise PipelineError('requested_id_not_in_inventory')
    if max_bytes <= 0:
        raise PipelineError('invalid_max_video_size')
    ids = list(dict.fromkeys([*first, *(only or sorted(known))]))
    jobs, deferred = [], []
    for mid in ids:
        job = known[mid]
        _, _, record, state = job
        if state.get('status') in ('blocked', 'skipped'):
            continue
        # An archived video whose cover is still outstanding stays selectable.
        if mid in archived and mid in covered and mid not in only:
            continue
        size, reason = record.get('expected_size'), None
        path = Path(root) / 'videos' / filename(record)
        # Its video object is already committed: only the cover is outstanding, and a
        # cover is fetched from the message, so none of the video's size/path limits apply.
        if mid in cover_only and mid in archived:
            jobs.append(job)
            continue
        if mid in defer_ids:
            reason = 'explicitly_deferred'
        elif not isinstance(size, int) or size <= 0:
            reason = 'unknown_size'
        elif bucket_limit is not None and size > bucket_limit:
            reason = 'bucket_file_limit'
        elif size > max_bytes:
            reason = 'large_video'
        elif not resume_partials and (path.with_name(path.name + '.part').exists()
                                     or path.with_name(path.name + '.part.resume').exists()):
            reason = 'existing_partial_preserved'
        if reason:
            deferred.append({'message_id': mid, 'bytes': size, 'reason': reason})
        else:
            jobs.append(job)
    return jobs, deferred


def retry_plan(rows, root, report_path, project_ref, *, include_deferred=False,
               max_bytes=128 * 1024**2, bucket_limit=None):
    """One finite selection from durable evidence, never from the first N rows.

    Lease expiry, attempt limits and backoff remain enforced by DB claim().
    Queued/in-progress evidence is a crash candidate, not an archived success.
    """
    report = json.loads(Path(report_path).read_text())
    if report.get('project_ref') != project_ref:
        raise PipelineError('retry_report_project_mismatch')
    parts = report.get('segments', [report])
    latest = {x['message_id']: x for p in parts for x in p.get('videos', [])}
    ids = [mid for mid, x in latest.items()
           if x['result'] not in SUCCESS or x.get('cover_result') not in COVER_SUCCESS]
    # A pass interrupted in its first segment has not created later segments yet.
    ids += [mid for mid in report.get('pending_message_ids', []) if mid not in ids]
    if include_deferred:
        ids += [x['message_id'] for x in report.get('deferred', []) if x['message_id'] not in ids]
    if not ids:
        return [], []
    return plan(rows, root, set(), only=ids, max_bytes=max_bytes, bucket_limit=bucket_limit,
                defer_ids=() if include_deferred else (3021,), resume_partials=True)


def claim_blocker(cloud):
    """Describe a stage that cannot be claimed without changing its DB state."""
    if not cloud or cloud.get('object_path'):
        return None
    tasks = cloud['tasks']
    task = tasks['upload'] if tasks['download']['status'] == 'succeeded' else tasks['download']
    if task.get('attempts', 0) >= 5:
        return 'attempts_exhausted'
    current = datetime.now(timezone.utc)
    if task['status'] == 'running' and task.get('lease_expires_at'):
        if datetime.fromisoformat(task['lease_expires_at']) > current:
            return 'lease_active'
    if task['status'] == 'failed' and task.get('updated_at'):
        if (current - datetime.fromisoformat(task['updated_at'])).total_seconds() < 30:
            return 'database_backoff'
    return None


def select(rows, limit, first=(), archived=frozenset()):
    """Explicit ids first, then unarchived videos by ascending message id.

    A local success is not archival: a download that succeeded but whose upload
    never committed must be picked up again, so `archived` (cloud-committed
    message ids) decides, with only blocked/skipped always excluded.
    """
    if type(limit) is not int or not 0 < limit <= MAX_BATCH:
        raise PipelineError('batch_limit_out_of_range')
    todo = {r['message_id']: (d, v, r, s) for d, v, r, s in rows
            if s.get('status') not in ('blocked', 'skipped') and r['message_id'] not in archived}
    ids = list(dict.fromkeys(first))
    if any(i not in todo for i in ids):
        raise PipelineError('first_id_not_pending')
    ids += sorted(i for i in todo if i not in ids)
    return [todo[i] for i in ids[:limit]]


def select_ai_candidates(rows, archived, limit=None):
    """Already-archived videos, ascending message id; claim() re-checks eligibility.

    archived is the cloud-committed message id set from archived_messages().
    A video already ai-succeeded, in backoff or attempts-exhausted is not
    filtered out here: run_ai's claim() rejects it and the caller skips it.
    """
    by_message = {r['message_id']: (d, v, r, s) for d, v, r, s in rows}
    ids = sorted(mid for mid in archived if mid in by_message)
    if limit is not None:
        if type(limit) is not int or limit <= 0:
            raise PipelineError('invalid_ai_limit')
        ids = ids[:limit]
    return [by_message[mid] for mid in ids]


def select_ai_drama_candidates(rows, archived, limit=None):
    """Dramas with at least one archived video, ascending by that drama's earliest
    archived message id; drama_archive.claim() re-checks eligibility against live
    DB state (this is only a cheap local pre-filter, same role as select_ai_candidates).

    Returns drama ids, not video tuples: the one video actually used for the AI
    call is picked later, inside the DB, at claim/run time -- not here.
    """
    earliest = {}
    for d, v, r, s in rows:
        if r['message_id'] in archived:
            did = v['drama_id']
            earliest[did] = min(earliest.get(did, r['message_id']), r['message_id'])
    ids = sorted(earliest, key=earliest.get)
    if limit is not None:
        if type(limit) is not int or limit <= 0:
            raise PipelineError('invalid_ai_limit')
        ids = ids[:limit]
    return ids


class Gate:
    """Concurrency limit that can be lowered while running (flood-wait response)."""
    def __init__(self, limit, stop=None):
        self.limit, self.active, self.peak = limit, 0, 0
        self.condition = asyncio.Condition()
        self.stop, self.paused_until = stop, 0

    async def __aenter__(self):
        async with self.condition:
            while True:
                if self.stop is not None and self.stop.is_set():
                    raise StopArchive('batch_stopped')
                if self.active < self.limit and time.monotonic() >= self.paused_until:
                    break
                try:
                    await asyncio.wait_for(self.condition.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass
            self.active += 1
            self.peak = max(self.peak, self.active)

    async def __aexit__(self, *exc):
        async with self.condition:
            self.active -= 1
            self.condition.notify_all()

    def reduce(self, seconds=0):
        self.limit = max(1, self.limit // 2)
        self.paused_until = max(self.paused_until, time.monotonic() + seconds)


async def process_batch(db, storage, cache, store, client, peer, source, jobs, *,
                        concurrency=4, cleanup=False, report_path=None, pause_wait=120,
                        download_timeout=1800, abort=None, project_ref=None, deferred=(),
                        resume_partials=False, cover_concurrency=2, cover_only=frozenset()):
    states = {r['message_id']: s for r, s in store.videos()}
    stop = asyncio.Event()
    abort = abort if abort is not None else threading.Event()
    downloads, uploads = Gate(concurrency, stop), Gate(concurrency, stop)
    # Covers refresh their own messages, so they add RPCs the video line never made.
    # A separate small gate keeps that extra pressure off the video download limit.
    covers = Gate(min(cover_concurrency, concurrency), stop)
    started = time.monotonic()
    report = {'at': now(), 'status': 'in_progress', 'requested': len(jobs), 'concurrency': concurrency,
              'cleanup': cleanup, 'flood_events': [], 'videos': [], 'ai_called': False,
              'stop_reason': None, 'project_ref': project_ref, 'deferred': list(deferred)}
    report['videos'] = [{'message_id': r['message_id'], 'video_id': v['id'],
                         'bytes': r['expected_size'], 'result': 'queued',
                         'cover_result': 'queued', 'timings': {}}
                        for _, v, r, _ in jobs]

    def save():
        report.update(peak_downloads=downloads.peak, peak_uploads=uploads.peak,
                      peak_covers=covers.peak, download_limit_now=downloads.limit,
                      elapsed_seconds=round(time.monotonic() - started, 1))
        report['counts'] = dict(Counter(x['result'] for x in report['videos']))
        report['cover_counts'] = dict(Counter(x['cover_result'] for x in report['videos']))
        if report_path:
            atomic_json(report_path, report)

    def flood(seconds):
        report['flood_events'].append({'at': now(), 'seconds': seconds})
        downloads.reduce(seconds + 1)
        if seconds > archive.MAX_FLOOD_WAIT:
            report['stop_reason'] = 'flood_wait'
            stop.set()

    def active():
        if abort.is_set():
            raise PipelineError('worker_stopping')

    def download_active():
        active()
        if stop.is_set():
            raise StopArchive('batch_stopped')

    async def threaded(fn, *args, **kwargs):
        # Cancellation must not release the project lock while upload threads still run.
        pending = asyncio.create_task(asyncio.to_thread(fn, *args, **kwargs))
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            abort.set()
            try:
                await asyncio.shield(pending)
            except Exception:
                pass
            raise

    async def progress():
        while True:
            await asyncio.sleep(15)
            save()

    async def with_cache(factory):
        for _ in range(max(1, pause_wait // 5)):
            active()
            if stop.is_set():
                raise StopArchive('batch_stopped')
            try:
                return await factory()
            except PipelineError as exc:
                if str(exc) != 'cache_paused':
                    raise
                await asyncio.sleep(5)
        raise PipelineError('cache_paused')

    async def process(job, item):
        d, v, r, _ = job
        began_item = time.monotonic()
        try:
            active()
            if stop.is_set():
                item['result'] = item['cover_result'] = 'not_started'
                return
            # One import for both lines; it also backfills the two cover task rows.
            await threaded(db.call, 'import_video', d, v)
        except asyncio.CancelledError:
            item['result'] = item['cover_result'] = 'interrupted'
            raise
        except Exception as exc:
            item.update(result='failed', cover_result='failed', error=error_code(exc))
            item['elapsed_seconds'] = round(time.monotonic() - began_item, 3)
            save()
            return
        # Independent tracks: a cover failure never touches the video and vice versa.
        try:
            await asyncio.gather(video_line(job, item), cover_line(job, item))
        finally:
            item['elapsed_seconds'] = round(time.monotonic() - began_item, 3)
            save()

    async def cover_line(job, item):
        _, v, r, _ = job
        mid, vid = r['message_id'], v['id']
        began = time.monotonic()
        try:
            active()
            if stop.is_set():
                item['cover_result'] = 'not_started'
                return
            cover = store.cover(mid)
            if cover['status'] == 'skipped':
                item['cover_result'] = 'skipped'
                return
            cloud = await threaded(db.state, vid)
            if 'cover_download' not in cloud['tasks'] or 'cover_upload' not in cloud['tasks']:
                raise PipelineError('cover_stages_missing')
            if cloud.get('cover_path'):
                item['cover_result'] = 'already_archived'
                return
            if cloud.get('cover_note'):
                item.update(cover_result='no_thumbnail', cover_error=cloud['cover_note'])
                return
            async with covers:
                active()
                if cloud['tasks']['cover_download']['status'] != 'succeeded':
                    item['cover_result'] = 'cover_downloading'
                    item['cover_result'] = await cover_download_task(
                        db, store, v, r, cover, client, peer, check=download_active)
                    if item['cover_result'] == 'no_thumbnail':
                        # The note is committed; record that so later passes stop selecting it.
                        cover = mark_cover_remote(store.cover(mid), project_ref,
                                                  note_committed=True, note_at=now())
                        store.update_cover(mid, cover)
                        return
                    cover = store.cover(mid)
                item['cover_result'] = 'cover_uploading'
                evidence = await threaded(cover_upload_task, db, storage, store.root, v, r,
                                          cover, check=active)
                item['cover_result'] = evidence.pop('result')
                item['cover_path'] = evidence['cover_path']
                # Evidence belongs to this project's bucket only, not to the shared local file.
                mark_cover_remote(cover, project_ref, **evidence)
                store.update_cover(mid, cover)
        except asyncio.CancelledError:
            item['cover_result'] = 'interrupted'
            raise
        except StopArchive as exc:
            code = str(exc).split(':')[0][:80]
            item.update(cover_result='not_started' if code == 'batch_stopped' else 'stopped',
                        cover_error=code)
            if code in ('disk_full', 'disk_insufficient'):
                report['stop_reason'] = code
                stop.set()
        except Exception as exc:
            # A cover never stops intake: the video line owns the stop conditions.
            item.update(cover_result='failed', cover_error=error_code(exc))
        finally:
            item['cover_seconds'] = round(time.monotonic() - began, 3)
            save()

    async def video_line(job, item):
        _, v, r, _ = job
        mid, vid = r['message_id'], v['id']
        try:
            active()
            if stop.is_set():
                item['result'] = 'not_started'
                return
            state = states[mid]
            cloud = await threaded(db.state, vid)
            if mid in cover_only and cloud.get('object_path'):
                # Selected for its cover alone: re-reading the whole committed
                # object would cost its full size in egress for nothing.
                item['result'] = 'video_already_committed'
                return
            blocker = claim_blocker(cloud)
            if blocker:
                item.update(result='retry_deferred', error=blocker)
                return
            if cloud['tasks']['upload']['status'] != 'succeeded' and cloud['tasks']['download']['status'] != 'succeeded':
                if stop.is_set():
                    item['result'] = 'not_started'
                    return
                async with downloads:
                    active()
                    began = time.monotonic()
                    item['result'] = 'downloading'
                    item['download'] = await with_cache(lambda: download_task(
                        db, cache, v, r, state, client, peer, source, store,
                        timeout_seconds=download_timeout, metrics=item['timings'], check=download_active,
                        resume_partial=resume_partials))
                    item['download_seconds'] = round(time.monotonic() - began, 1)
                if item['download'] == 'cloud_archived_skipped':
                    item['download'] = None
            async with uploads:
                active()
                item['result'] = 'uploading_or_verifying'
                item['result'] = await threaded(archive_existing, db, storage, cache, v, r, state, False,
                                                check=active, metrics=item['timings'])
            item.update(await threaded(sample_evidence, db, cache, v, r))
            active()
            entry = await threaded(cache.get, vid)
            if cleanup and not entry.get('protected') and entry.get('path'):
                await threaded(cache.cleanup, vid, db)
                item['local_cleaned'] = True
        except asyncio.CancelledError:
            item['result'] = 'interrupted'
            raise
        except StopArchive as exc:
            code = str(exc).split(':')[0][:80]
            item.update(result='not_started' if code == 'batch_stopped' else 'stopped', error=code)
            if code in ('flood_wait', 'disk_full', 'disk_insufficient'):
                report['stop_reason'] = code
                stop.set()
        except Exception as exc:
            item.update(result='failed', error=error_code(exc))
            if item['error'] == 'psycopg_OperationalError':
                # An isolated connection error is a file failure. Stop intake only
                # if a fresh health query also fails after bounded connect retries.
                try:
                    await threaded(db.archived_messages)
                except Exception:
                    report['stop_reason'] = 'database_unavailable'
                    stop.set()
            if item['error'] in ('cache_paused', 'remote_object_conflict',
                                  'archive_identity_conflict', 'bucket_configuration_conflict',
                                  'storage_permission_unavailable', 'psycopg_InsufficientPrivilege'):
                report['stop_reason'] = item['error']
                stop.set()
        finally:
            save()

    archive.on_flood = flood
    queue = iter(zip(jobs, report['videos']))
    async def worker():
        for job, item in queue:
            await process(job, item)
    # Bound the complete in-flight lifecycle, including imports and state reads.
    # No segment-wide burst of DB requests can crowd out heartbeat work.
    tasks = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(jobs)))]
    ticker = asyncio.create_task(progress())
    save()
    try:
        await asyncio.gather(*tasks)
        ok = [x for x in report['videos'] if x['result'] in SUCCESS]
        report['status'] = 'all_archived' if len(ok) == len(jobs) else 'incomplete'
    except BaseException:
        abort.set()
        report['status'] = 'interrupted'
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)
        archive.on_flood = None
        save()
    return report


async def process_pass(db, storage, cache, store, client, peer, source, jobs, *,
                       batch_size=100, report_path=None, deferred=(), project_ref=None,
                       retry_rounds=0, **options):
    if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH:
        raise PipelineError('batch_limit_out_of_range')
    report = {'at': now(), 'status': 'in_progress', 'project_ref': project_ref,
              'requested': len(jobs), 'deferred': list(deferred), 'segments': [], 'ai_called': False,
              'pending_message_ids': [j[2]['message_id'] for j in jobs]}
    started = time.monotonic()
    def save():
        report['elapsed_seconds'] = round(time.monotonic() - started, 1)
        latest = {x['message_id']: x for p in report['segments'] for x in p['videos']}
        report['counts'] = dict(Counter(x['result'] for x in latest.values()))
        report['cover_counts'] = dict(Counter(x.get('cover_result') for x in latest.values()))
        if report_path:
            atomic_json(report_path, report)
    save()
    try:
        for number, offset in enumerate(range(0, len(jobs), batch_size), 1):
            segment_path = (report_path.with_name(report_path.stem + f'-segment-{number}.json')
                            if report_path else None)
            report['active_segment_report'] = str(segment_path) if segment_path else None
            save()
            part = await process_batch(db, storage, cache, store, client, peer, source,
                                       jobs[offset:offset + batch_size], report_path=segment_path,
                                       project_ref=project_ref, **options)
            report['segments'].append(part)
            report['pending_message_ids'] = [j[2]['message_id'] for j in jobs[offset + len(part['videos']):]]
            save()
            if part.get('stop_reason'):
                report.update(status='stopped', stop_reason=part['stop_reason'])
                break
            options['concurrency'] = part['download_limit_now']
        else:
            for retry_round in range(retry_rounds):
                latest = {x['message_id']: x for p in report['segments'] for x in p['videos']}
                retry_jobs = [j for j in jobs if latest.get(j[2]['message_id'], {}).get('result') not in SUCCESS]
                if not retry_jobs:
                    break
                report['status'] = 'retry_backoff'
                save()
                await asyncio.sleep(30)  # DB backoff; one finite sweep after the whole normal pass.
                for number, offset in enumerate(range(0, len(retry_jobs), batch_size), 1):
                    retry_path = (report_path.with_name(report_path.stem + f'-retry-{retry_round + 1}-segment-{number}.json')
                                  if report_path else None)
                    report['active_segment_report'] = str(retry_path) if retry_path else None
                    save()
                    part = await process_batch(db, storage, cache, store, client, peer, source,
                                               retry_jobs[offset:offset + batch_size], report_path=retry_path,
                                               project_ref=project_ref, **{**options, 'resume_partials': True})
                    part['retry_round'] = retry_round + 1
                    report['segments'].append(part)
                    save()
                    if part.get('stop_reason'):
                        report.update(status='stopped', stop_reason=part['stop_reason'])
                        return report
            latest = {x['message_id']: x for p in report['segments'] for x in p['videos']}
            failed = any(x['result'] not in SUCCESS for x in latest.values())
            report['status'] = 'incomplete' if failed else ('normal_queue_archived' if deferred else 'all_archived')
    except BaseException:
        report['status'] = 'interrupted'
        raise
    finally:
        save()
    return report


async def run_batch(config_values, db, storage, cache, store, jobs, *, batch_size=None, **options):
    """Connect the confirmed whitelisted Telegram source, then run the bounded batch."""
    abort = threading.Event()
    client = archive.client_for(archive.load_config())
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    import signal
    def cancel():
        abort.set()
        task.cancel()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, cancel)
    try:
        await archive.rpc(client.connect)
        if not await archive.rpc(client.is_user_authorized):
            raise PipelineError('telegram_login_required')
        if await archive.rpc(client.is_bot):
            raise PipelineError('user_account_required')
        peer, source = await archive.confirmed_target(client)
        if batch_size is not None:
            return await process_pass(db, storage, cache, store, client, peer, source, jobs,
                                      batch_size=batch_size, abort=abort, **options)
        return await process_batch(db, storage, cache, store, client, peer, source, jobs, abort=abort, **options)
    finally:
        if client.is_connected():
            await asyncio.wait_for(client.disconnect(), timeout=20)


async def drain_transcode(db, storage, cache, candidates, *, concurrency=2, report_path=None):
    """Bounded, finite drain of the transcode backlog.

    plan() already excludes a fully-archived-and-covered video from future jobs
    (nothing left for it to do), so the transcode backlog cannot ride along inside
    video_line/cover_line's per-job gather -- those videos would never be selected
    into a pass in the first place. This runs as its own sweep over every already
    -archived video instead, independent of the video/cover job list. No Telegram
    connection is needed: it only rewraps an object this project already owns.
    """
    abort = threading.Event()
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    def cancel():
        abort.set()
        task.cancel()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, cancel)
    gate = Gate(concurrency)
    started = time.monotonic()
    report = {'at': now(), 'status': 'in_progress', 'requested': len(candidates), 'results': []}

    def save():
        report['counts'] = dict(Counter(x['result'] for x in report['results']))
        report['elapsed_seconds'] = round(time.monotonic() - started, 1)
        if report_path:
            atomic_json(report_path, report)

    def active():
        if abort.is_set():
            raise PipelineError('worker_stopping')

    async def threaded(fn, *args, **kwargs):
        pending = asyncio.create_task(asyncio.to_thread(fn, *args, **kwargs))
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            abort.set()
            try:
                await asyncio.shield(pending)
            except Exception:
                pass
            raise

    async def one(v, r):
        entry = {'message_id': r['message_id'], 'video_id': v['id']}
        try:
            async with gate:
                active()
                entry['result'] = await threaded(transcode_task, db, storage, cache, v['id'], check=active)
        except asyncio.CancelledError:
            entry['result'] = 'interrupted'
            raise
        except PipelineError as exc:
            entry.update(result='skipped' if str(exc) == 'task_not_claimable' else 'failed', error=str(exc))
        except Exception as exc:
            entry.update(result='failed', error=error_code(exc))
        finally:
            report['results'].append(entry)
            save()

    queue = iter((v, r) for _, v, r, _ in candidates)
    async def worker():
        for v, r in queue:
            await one(v, r)
    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(candidates)))] if candidates else []
    save()
    try:
        await asyncio.gather(*workers)
        report['status'] = 'drained'
    except BaseException:
        report['status'] = 'interrupted'
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise
    finally:
        save()
    return report
