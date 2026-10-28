#!/usr/bin/env python3
"""Finite archive passes with project-scoped storage state; no daemon or real AI provider."""
import argparse
import asyncio
import fcntl
import json
import os
import re
import signal
import sqlite3
import sys
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from video_archive.core import atomic_json, cover_filename, filename, now, sha256, validate_file
from video_archive.pipeline.cache import Cache, legacy_entry
from video_archive.pipeline.common import (BASE, PipelineError, ai_output, config, cover_object_path,
                             error_code, identity, object_path, snapshot)
from video_archive.pipeline.db import Database, Lease
from video_archive.pipeline.storage import Storage
from video_archive.pipeline.outcomes import outcome_file, filter_approved_rows
from video_archive.paths import cli_path
from video_archive.screening.quality import QualityPolicy

TOPICS = BASE / 'archive/topics-20261019-184029-928673/topics.json'
SAMPLES = {6558, 6559}


def drama_selection_summary(evaluations):
    """Compact CLI summary; the report retains each complete drama decision."""
    from collections import Counter
    return {'evaluated_dramas': len(evaluations),
            'quality_decisions': dict(Counter(x['decision'] for x in evaluations)),
            'planning_decisions': dict(Counter(x.get('planning_status', x['decision'])
                                               for x in evaluations)),
            'accepted_drama_videos': sum(x['video_count'] for x in evaluations
                                         if x['decision'] == 'accept')}


def download_quality_policy(args):
    """Recovery defaults to the same versioned policy as its original pass."""
    if args.command == 'retry' and args.minimum_quality is None and args.report:
        report = json.loads(args.report.read_text())
        stored = report.get('quality_policy')
        if stored:
            if not isinstance(stored, dict) or stored.get('version') != 'drama_quality_v1':
                raise PipelineError('retry_quality_policy_conflict')
            return QualityPolicy(**{key: value for key, value in stored.items() if key != 'version'})
    return QualityPolicy(minimum_grade={480: 1, 720: 2, 1080: 3}[args.minimum_quality or 720])


def local_file(root, record, state):
    path = Path(root) / 'videos' / filename(record)
    if (path.is_symlink() or not path.resolve().is_relative_to(Path(root).resolve())
            or state.get('local_path') != str(path.relative_to(root))):
        raise PipelineError('local_path_identity_mismatch')
    if state.get('status') != 'success' or not state.get('sha256'):
        raise PipelineError('local_sample_not_verified')
    return path


def cover_file(root, record, state):
    path = Path(root) / 'covers' / cover_filename(record)
    if (path.is_symlink() or not path.resolve().is_relative_to(Path(root).resolve())
            or state.get('local_path') != str(path.relative_to(root))):
        raise PipelineError('cover_path_identity_mismatch')
    if state.get('status') != 'success' or not state.get('sha256'):
        raise PipelineError('local_cover_not_verified')
    return path


def import_rows(db, rows):
    for drama, video, _, _ in rows:
        db.call('import_video', drama, video)


def verify_identity(video, record, cloud):
    expected = identity('message', record['source']['id'], record['message_id'])
    if not cloud or video['id'] != expected:
        raise PipelineError('archive_identity_conflict')
    for key in ('id', 'drama_id', 'source_id', 'message_id', 'media_id'):
        if str(cloud.get(key)) != str(video[key]):
            raise PipelineError('archive_identity_conflict')
    if any(int(video[key]) != int(value) for key, value in (
            ('source_id', record['source']['id']), ('message_id', record['message_id']),
            ('media_id', record['media_id']))):
        raise PipelineError('archive_identity_conflict')



def committed_inventory(db, rows):
    """Confirm source-specific committed members without reading media bytes.

    The legacy index contains message ids across sources. A matching number is
    only a candidate; the full video identity and committed stages must agree.
    """
    candidates = set(db.archived_messages())
    jobs = [job for job in rows if job[2]['message_id'] in candidates]

    def inspect(job):
        _, video, record, _ = job
        cloud = db.state(video['id'])
        if not cloud:
            return None
        verify_identity(video, record, cloud)
        path = cloud.get('object_path')
        if path and path != object_path(video['id']):
            raise PipelineError('archive_identity_conflict')
        cover_path = cloud.get('cover_path')
        if cover_path and cover_path != cover_object_path(video['id']):
            raise PipelineError('archive_identity_conflict')
        tasks = cloud.get('tasks') or {}
        if not path or (tasks.get('upload') or {}).get('status') != 'succeeded':
            return None
        cover_done = (all((tasks.get(stage) or {}).get('status') == 'succeeded'
                          for stage in ('cover_download', 'cover_upload'))
                      and bool(cover_path or cloud.get('cover_note')))
        return record['message_id'], cover_done

    if not jobs:
        return set(), set()
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = [receipt for receipt in pool.map(inspect, jobs) if receipt is not None]
    return {mid for mid, _ in receipts}, {mid for mid, covered in receipts if covered}


def sample_evidence(db, cache, video, record):
    """ai_status is reported, not required 'pending': the AI result is shared by the
    whole drama, so a sibling episode can already have triggered it by the time
    this video's own upload finishes.
    """
    cloud, entry = db.state(video['id']), cache.get(video['id'])
    verify_identity(video, record, cloud)
    if (cloud.get('object_path') != object_path(video['id'])
            or cloud['tasks']['upload']['status'] != 'succeeded'
            or not entry.get('sha256') or entry.get('sha256') != entry.get('remote_sha256')):
        raise PipelineError('sample_acceptance_conflict')
    return {'message_id': record['message_id'], 'video_id': video['id'],
            'drama_id': video['drama_id'], 'object_path': cloud['object_path'],
            'bytes': entry['size'], 'local_sha256': entry['sha256'],
            'remote_sha256': entry['remote_sha256'], 'upload_status': 'succeeded',
            'ai_status': cloud['tasks']['ai']['status'], 'remote_verified_at': entry['remote_verified_at']}


def archive_existing(db, storage, cache, video, record, state, decode=True, *, check=lambda: None, metrics=None):
    vid = video['id']
    with cache.lock(vid):
        check()
        cloud = db.state(vid)
        verify_identity(video, record, cloud)
        if cloud and cloud['tasks']['upload']['status'] == 'succeeded':
            if cloud['object_path'] != object_path(vid):
                raise PipelineError('archive_state_conflict')
            entry = cache.get(vid)
            # Missing cache is not a reason to ask Telegram for the file.
            expected = entry.get('sha256') or state.get('sha256')
            size = entry.get('size') or record['expected_size']
            actual = storage.readback(vid, size, check, **({'metrics': metrics} if metrics is not None else {}))
            if not expected or actual != expected:
                raise PipelineError('remote_sha256_mismatch')
            check()
            cache.put(vid, archived=True, sha256=expected, size=size,
                      remote_sha256=actual, object_path=object_path(vid),
                      remote_verified_at=now(), protected=record['message_id'] in SAMPLES)
            return 'already_archived_readback_verified'
        path = local_file(cache.root, record, state)
        began = time.monotonic()
        result = validate_file(path, record['expected_size'], state['sha256'], decode=decode)
        if metrics is not None:
            metrics['upload_validation_seconds'] = round(time.monotonic() - began, 3)
        check()
        cache.put(vid, path=str(path), size=result['actual_size'], sha256=result['sha256'],
                  protected=record['message_id'] in SAMPLES, local_verified_at=now())
        if cloud['tasks']['download']['status'] != 'succeeded':
            with Lease(db, vid, 'download') as lease:
                lease.finish()
        with Lease(db, vid, 'upload') as lease:
            def active():
                check()
                lease.check()
            actual = storage.upload(vid, path, result['sha256'], cache, active,
                                    record.get('mime_type') or 'application/octet-stream',
                                    **({'metrics': metrics} if metrics is not None else {}))
            active()
            cache.put(vid, remote_sha256=actual, remote_verified_at=now())
            active()
            lease.finish({'object_path': object_path(vid)})
            cache.put(vid, archived=True, object_path=object_path(vid))
        return 'uploaded_readback_verified'


async def download_task(db, cache, video, record, state, client, peer, source, store, *,
                        timeout_seconds=1800, metrics=None, check=lambda: None, resume_partial=False):
    """Fenced download, using the target project's DB rather than legacy tombstones.

    The orchestrator must hold the shared project process lock.
    """
    from video_archive.telegram.archive import download_one
    vid = video['id']
    with cache.lock(vid):
        check()
        cloud = await asyncio.to_thread(db.state, vid)
        if cloud['object_path'] and cloud['tasks']['upload']['status'] == 'succeeded':
            return 'cloud_archived_skipped'
        with cache.reserve(vid, record['expected_size']):
            async with Lease(db, vid, 'download') as lease:
                def active():
                    check()
                    lease.check()

                class FencedStore:
                    root = store.root
                    cloud_state_checked = True  # Current project's DB was checked above; ignore legacy test tombstones.

                    def update(self, mid, value):
                        lease.fence()
                        store.update(mid, value)

                    async def update_async(self, mid, value):
                        await asyncio.to_thread(lease.fence)
                        store.update(mid, value)

                result = await download_one(client, peer, source, FencedStore(), record, state,
                                            timeout_seconds=timeout_seconds, retries=1,
                                            preserve_partial=True, resume_partial=resume_partial,
                                            metrics=metrics, check=active)
                if result not in ('downloaded_success', 'verified_skipped'):
                    raise PipelineError('download_not_verified')
                lease.check()
                path = local_file(cache.root, record, state)
                cache.put(vid, path=str(path), size=state['actual_size'], sha256=state['sha256'])
                await asyncio.to_thread(lease.finish)
                return result


async def cover_download_task(db, store, video, record, cover_state, client, peer, *,
                              check=lambda: None, timeout_seconds=120):
    """Fenced cover_download: Telegram's own thumbnail, stored beside the video.

    Takes no cache lock or reservation on purpose. Covers live in their own
    directory, are bounded by the bucket's 5 MiB limit, and must stay claimable
    while the same video's download/upload lease is held by the video line.
    """
    from video_archive.telegram.archive import download_cover
    vid = video['id']
    async with Lease(db, vid, 'cover_download') as lease:
        def active():
            check()
            lease.check()

        async def persist(mid, state):
            await asyncio.to_thread(lease.fence)
            store.update_cover(mid, state)

        result = await download_cover(client, peer, store, record, cover_state, check=active,
                                      timeout_seconds=timeout_seconds, persist=persist)
        lease.check()
        if result == 'no_thumbnail':
            # Terminal and recorded, not a failure: finish() closes cover_upload too.
            await asyncio.to_thread(lease.finish, {'cover_note': cover_state['failure_reason']})
            return result
        if result not in ('downloaded_success', 'verified_skipped'):
            raise PipelineError('cover_not_downloaded')
        await asyncio.to_thread(lease.finish)
        return result


def cover_upload_task(db, storage, root, video, record, cover_state, *, check=lambda: None):
    """Fenced cover_upload into the public cover bucket, then commit cover_path.

    Runs in a worker thread, so it returns the evidence instead of writing it:
    the legacy SQLite handle belongs to the event loop thread alone.
    """
    vid = video['id']
    path = cover_file(root, record, cover_state)
    with Lease(db, vid, 'cover_upload') as lease:
        def active():
            check()
            lease.check()
        actual = storage.upload_cover(vid, path, cover_state['sha256'], active)
        active()
        lease.finish({'cover_path': cover_object_path(vid)})
        return {'result': 'cover_uploaded_readback_verified', 'remote_sha256': actual,
                'remote_verified_at': now(), 'cover_path': cover_object_path(vid)}


def transcode_task(db, storage, cache, vid, *, check=lambda: None):
    """Rewrap an already-archived object so its moov atom is at the front.

    object_path never changes -- the cloud object is overwritten in place.
    Downloads the current object, remuxes losslessly (ffmpeg -c copy), and only
    overwrites the cloud copy when the bytes actually changed, so a video that
    was already faststart costs one readback and no upload.
    """
    from video_archive.pipeline.transcode import remux_faststart
    scratch = cache.directory / 'scratch'
    scratch.mkdir(mode=0o700, exist_ok=True)
    original = scratch / f'{vid}.transcode-original'
    remuxed = scratch / f'{vid}.transcode-faststart'
    with Lease(db, vid, 'transcode') as lease:
        def active():
            check()
            lease.check()
        try:
            # A locally cached size/sha256 can be stale for reasons that have
            # nothing to do with transcoding, so ask the object itself rather
            # than trust the cache as the basis for what we are about to rewrap.
            size = storage.remote_size(vid, active)
            if size is None:
                raise PipelineError('remote_object_missing')
            with original.open('wb') as handle:
                sha = storage.readback(vid, size, active, handle)
            if sha is None:
                raise PipelineError('remote_object_missing')
            active()
            new_sha = remux_faststart(original, remuxed, check=active)
            if new_sha == sha:
                cache.put(vid, sha256=sha, size=size, remote_sha256=sha, remote_verified_at=now())
                lease.finish()
                return 'already_faststart_skipped'
            active()
            uploaded = storage.replace(vid, remuxed, new_sha, active)
            active()
            cache.put(vid, sha256=new_sha, size=remuxed.stat().st_size,
                      remote_sha256=uploaded, remote_verified_at=now())
            lease.finish()
            return 'transcoded_readback_verified'
        finally:
            original.unlink(missing_ok=True)
            remuxed.unlink(missing_ok=True)


@contextmanager
def ai_input(db, storage, cache, vid, check):
    """Exclusive OS read protection also prevents cleanup; cloud fallback bounded."""
    with cache.lock(vid):
        # Fill gaps from the pre-project-scoping cache; the live project's own
        # record (if any) always wins. Never used to decide archived/not-archived.
        entry = {**legacy_entry(cache.root, vid), **cache.get(vid)}
        if entry.get('path'):
            path = cache.path(entry)
            if path.exists() and sha256(path) == entry.get('sha256'):
                yield path
                return
        if not entry.get('size') or not entry.get('sha256'):
            raise PipelineError('local_integrity_record_missing')
        state = db.state(vid)
        if not state.get('object_path') or state['tasks']['upload']['status'] != 'succeeded':
            raise PipelineError('archive_not_committed')
        scratch = cache.directory / 'scratch'
        scratch.mkdir(mode=0o700, exist_ok=True)
        path = scratch / str(vid)
        # Interrupted scratch is disposable; never touch legacy originals.
        path.unlink(missing_ok=True)
        with cache.reserve(vid, entry['size']):
            try:
                with path.open('wb') as out:
                    actual = storage.readback(vid, entry['size'], check, out)
                if actual != entry['sha256']:
                    raise PipelineError('remote_sha256_mismatch')
                yield path
            finally:
                path.unlink(missing_ok=True)


def run_ai(db, storage, cache, did, provider, *, allow_test_provider=False):
    """did is a drama_id: the result is shared by every video under it, generated
    from just one representative video (the earliest-message_id one already
    uploaded), not every episode aggregated together.
    """
    # Provider must be explicitly approved by the caller; no model selected here.
    if getattr(provider, 'is_test_double', False) and not allow_test_provider:
        raise PipelineError('test_provider_forbidden')
    if allow_test_provider:
        from psycopg.conninfo import conninfo_to_dict
        info = conninfo_to_dict(db.dsn)
        if (info.get('host') != '127.0.0.1' or info.get('port') != '55440'
                or info.get('dbname') != 'drama_pipeline_test'
                or any(k in info for k in ('hostaddr', 'service'))
                or any(os.environ.get(k) for k in ('PGHOSTADDR', 'PGSERVICE'))):
            raise PipelineError('test_provider_forbidden')
    with Lease(db, did, 'ai') as lease:
        vid = db.call('ai_source_video', did)
        if not vid:
            raise PipelineError('ai_source_video_missing')
        vid = str(vid)
        from video_archive.pipeline.ai import usage_subject
        token = usage_subject.set({'drama_id': did, 'video_id': vid})
        try:
            with ai_input(db, storage, cache, vid, lease.check) as path:
                output = ai_output(provider.analyze(path, check=lease.check))
                lease.finish({**output, 'source_video_id': vid})
        finally:
            usage_subject.reset(token)


def backup_legacy(root):
    """SQLite backup API: consistent backup including WAL; original schema unchanged."""
    dest = BASE / '.private/implementation-backup' / ('state-' + now().replace(':', '-') + '.sqlite3')
    dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    source = sqlite3.connect(f'{(Path(root) / "state.sqlite3").as_uri()}?mode=ro', uri=True)
    target = sqlite3.connect(dest)
    try:
        source.backup(target)
        if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise PipelineError('backup_integrity_failed')
    finally:
        source.close()
        target.close()
    dest.chmod(0o600)
    return dest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=BASE / '.private/pipeline.json')
    parser.add_argument('--data', type=Path, default=BASE / 'archive')
    parser.add_argument('--topics', type=Path, default=TOPICS)
    parser.add_argument('command', choices=['init-config', 'check', 'inventory', 'plan', 'verify-samples', 'migrate', 'bucket', 'import', 'samples', 'batch', 'run', 'retry', 'repair', 'ai', 'transcode-backlog'])
    parser.add_argument('--report', type=Path, help='plan: save full decisions; retry: resume a report for this target project')
    parser.add_argument('--migration', help='migrate: one incremental file name under migrations/')
    parser.add_argument('--include-deferred', action='store_true', help='retry: explicitly include deferred with the chosen size threshold')
    parser.add_argument('--retry-rounds', type=int, default=4, choices=range(0, 5), help='download: retry rounds within the same drama channel (default 4)')
    parser.add_argument('--limit', type=int, help='download: soft video target, finish the current drama; ai/transcode-backlog: cap candidates')
    parser.add_argument('--drama-concurrency', type=int, default=1, choices=range(1, 9),
                        help='concurrent drama channels; file concurrency remains global')
    parser.add_argument('--drama-limit', type=int, help='download: maximum number of whole dramas to schedule')
    parser.add_argument('--minimum-quality', type=int, choices=(480, 720, 1080),
                        help='minimum drama quality tier (default 720; retry reuses its saved policy)')
    parser.add_argument('--first', type=int, nargs='*', default=[], help='download: process these messages\' complete dramas first')
    parser.add_argument('--only', type=int, nargs='+', default=[], help='download: select these messages\' complete dramas; explicit ids may be reverified')
    parser.add_argument('--max-video-mib', type=int, default=128, help='defer larger files (default 128 MiB)')
    parser.add_argument('--download-timeout', type=int, default=1800, help='per-file timeout seconds; one transfer attempt per pass')
    parser.add_argument('--batch-size', type=int, default=100, help='download: videos per segment (1..500; default 100)')
    parser.add_argument('--concurrency', type=int, default=4, choices=range(1, 9))
    parser.add_argument('--transcode-concurrency', type=int, default=2, choices=range(1, 5),
                        help='run: concurrent transcode-backlog workers after the normal pass')
    parser.add_argument('--cleanup', action='store_true', help='batch: delete local file after verified cloud commit')
    parser.add_argument('--cache-high-gb', type=int, default=10, choices=range(1, 41))
    args = parser.parse_args()
    for name in ('config', 'data', 'topics', 'report'):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, cli_path(value))
    os.umask(0o077)
    try:
        if args.command == 'init-config':
            args.config.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if args.config.parent.stat().st_mode & 0o077 or args.config.parent.is_symlink():
                raise PipelineError('private_config_permissions')
            # Exclusive creation, including symlink targets: never replace a config.
            try:
                with args.config.open('x') as handle:
                    handle.write((BASE / 'config/examples/pipeline.config.example.json').read_text())
            except FileExistsError:
                raise PipelineError('private_config_already_exists') from None
            print('private_config_template_created_no_remote_access')
            return 0
        if args.command == 'check':
            c = config(args.config)
            # Do not infer live plan/region from supplied values.
            storage = Storage(c)
            print(json.dumps({'configuration': 'valid', 'live_project_verified': False,
                              'database': Database(c['database_dsn']).verify_worker(),
                              'bucket': storage.bucket(), 'cover_bucket': storage.cover_bucket()}))
            return 0
        rows, excluded = snapshot(args.data, args.topics,
                                  require_complete=args.command in ('plan', 'batch', 'run', 'retry', 'repair', 'ai', 'transcode-backlog'))
        samples = [row for row in rows if row[2]['message_id'] in SAMPLES]
        if args.command == 'inventory':
            print(json.dumps({'eligible_videos': len(rows), 'dramas': len({r[0]['id'] for r in rows}),
                              'excluded': excluded, 'remote_access': False}))
            return 0
        if args.command in ('plan', 'batch', 'run', 'retry', 'repair'):
            if args.command == 'retry' and (args.report is None or args.only or args.first):
                raise PipelineError('retry_report_required_no_manual_ids')
            if not 1 <= args.max_video_mib <= 5120 or not 30 <= args.download_timeout <= 86400:
                raise PipelineError('invalid_download_limits')
            if not 1 <= args.batch_size <= 500:
                raise PipelineError('batch_limit_out_of_range')
            if args.drama_limit is not None and args.drama_limit <= 0:
                raise PipelineError('invalid_drama_limit')
            if args.command == 'batch' and ((args.limit is None and args.drama_limit is None)
                                            or (args.limit is not None and not 1 <= args.limit <= 500)):
                raise PipelineError('batch_limit_out_of_range')
            if args.limit is not None and args.limit <= 0:
                raise PipelineError('invalid_run_limit')
            quality_policy = download_quality_policy(args)
            if args.command == 'plan':
                from video_archive.pipeline.batch import plan_dramas
                jobs, deferred, evaluations = plan_dramas(
                    rows, args.data, set(), quality_policy=quality_policy,
                    only=args.only, first=args.first, max_bytes=args.max_video_mib * 1024**2,
                    video_limit=args.limit, drama_limit=args.drama_limit)
                from collections import Counter
                summary = {'eligible_before_cloud_check': len(jobs),
                           'bytes': sum(j[2]['expected_size'] for j in jobs),
                           'deferred_count': len(deferred),
                           'deferred_reasons': dict(Counter(x['reason'] for x in deferred)),
                           'max_video_mib': args.max_video_mib, 'remote_access': False,
                           'quality_policy': quality_policy.to_dict(),
                           **drama_selection_summary(evaluations)}
                if args.report:
                    atomic_json(args.report, {'at': now(), 'inventory_scope': 'complete_source_topics_frozen_boundary',
                                             **summary, 'dramas': evaluations, 'deferred': deferred})
                    summary['report'] = str(args.report)
                print(json.dumps(summary))
                return 0
        if args.command in ('samples', 'verify-samples') and len(samples) != 2:
            raise PipelineError('sample_inventory_incomplete')
        private = BASE / '.private'
        private.mkdir(mode=0o700, exist_ok=True)
        if args.command == 'verify-samples':
            lock_path = private / 'process.lock'
        else:
            # Locked per target project: a main worker and a test worker must be able
            # to run at once. Two workers on the *same* project still serialize.
            c = config(args.config)
            lock_path = private / f"process-{c['project_ref']}.lock"
        with lock_path.open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PipelineError('archive_worker_running') from None
            if args.command == 'verify-samples':
                results = []
                for _, v, r, s in samples:
                    result = validate_file(local_file(args.data, r, s), r['expected_size'], s['sha256'], decode=True)
                    results.append({'message_id': r['message_id'], 'video_id': v['id'], **result})
                report = {'at': now(), 'local_samples': results, 'remote_verified': False,
                          'original_three_sample_gate_changed': False}
                atomic_json(args.data / 'pipeline-local-acceptance.json', report)
                print(json.dumps({'local_verified': [x['message_id'] for x in results], 'remote_verified': False}))
                return 0
            db, storage = Database(c['database_dsn']), Storage(c)
            if args.command == 'samples' and max(r[2]['expected_size'] for r in samples) > min(c['global_file_limit'], c['bucket_file_limit']):
                raise PipelineError('sample_exceeds_project_limit')
            if args.command == 'migrate':
                # Configure owner DSN temporarily for this operation, then restricted login.
                import psycopg
                names = ('001_pipeline.sql', '002_archived_messages.sql', '003_import_video_concurrency.sql')
                if args.migration:
                    # Incremental migration onto a live schema: one explicitly named
                    # file, never a replay of the initial set.
                    if not re.fullmatch(r'0[0-9]{2}_[a-z0-9_]+\.sql', args.migration):
                        raise PipelineError('invalid_migration_name')
                    if args.migration in names:
                        raise PipelineError('initial_migration_not_incremental')
                    path = BASE / 'migrations' / args.migration
                    if not path.is_file() or path.is_symlink():
                        raise PipelineError('migration_file_missing')
                    names = (args.migration,)
                with psycopg.connect(c['database_dsn']) as connection:
                    exists = connection.execute("SELECT to_regnamespace('drama_archive')").fetchone()[0]
                    if bool(exists) is not bool(args.migration):
                        raise PipelineError('existing_schema_requires_manual_review'
                                            if exists else 'base_migration_required_first')
                    for name in names:
                        connection.execute((BASE / 'migrations' / name).read_text())
                print('migration_applied')
            elif args.command in ('batch', 'run', 'retry', 'repair'):
                from video_archive.pipeline.batch import run_batch, plan_dramas, retry_plan_dramas
                permissions = db.verify_worker()
                storage.bucket()
                storage.cover_bucket()
                archived, covered = committed_inventory(db, rows)
                # Carried for their cover only: their video object is already committed.
                cover_only = {mid for mid in archived if mid not in covered}
                if args.command == 'retry':
                    jobs, deferred, evaluations = retry_plan_dramas(
                        rows, args.data, args.report, c['project_ref'], quality_policy=quality_policy,
                        archived=archived, covered=covered, cover_only=cover_only,
                        include_deferred=args.include_deferred,
                        max_bytes=args.max_video_mib * 1024**2, bucket_limit=storage.limit,
                        video_limit=args.limit, drama_limit=args.drama_limit)
                else:
                    jobs, deferred, evaluations = plan_dramas(
                        rows, args.data, archived, quality_policy=quality_policy,
                        only=args.only, first=args.first, max_bytes=args.max_video_mib * 1024**2,
                        bucket_limit=storage.limit, covered=covered, cover_only=cover_only,
                        video_limit=args.limit, drama_limit=args.drama_limit,
                        resume_partials=args.command == 'repair',
                        defer_ids=() if args.command == 'repair' else (3021,),
                        force_readback=args.command != 'repair')
                selection_path = args.data / ('pipeline-selection-' + now().replace(':', '-') + '.json')
                atomic_json(selection_path, {'at': now(), 'project_ref': c['project_ref'],
                                            'inventory_scope': 'complete_source_topics_frozen_boundary',
                                            'quality_policy': quality_policy.to_dict(),
                                            **drama_selection_summary(evaluations), 'dramas': evaluations})
                backup_legacy(args.data)
                gib = 1024 ** 3
                cache = Cache(args.data, high=args.cache_high_gb * gib, low=args.cache_high_gb * gib * 6 // 10,
                              project_ref=c['project_ref'])
                from video_archive.core import Store
                store = Store(args.data)
                path = args.data / ('pipeline-' + args.command + '-' + now().replace(':', '-') + '.json')
                for evaluation in evaluations:
                    evaluation['report_path'] = str(path)
                try:
                    report = asyncio.run(run_batch(c, db, storage, cache, store, jobs,
                                                   concurrency=args.concurrency, cleanup=args.cleanup,
                                                   report_path=path, download_timeout=args.download_timeout,
                                                   cover_only=cover_only,
                                                   drama_evaluations=evaluations,
                                                   quality_policy_summary=quality_policy.to_dict(),
                                                   batch_size=args.batch_size,
                                                   retry_rounds=args.retry_rounds,
                                                   drama_concurrency=args.drama_concurrency,
                                                   outcome_path=outcome_file(args.data, c['project_ref']),
                                                   resume_partials=args.command in ('retry', 'repair'),
                                                   project_ref=c['project_ref'], deferred=deferred))
                finally:
                    store.db.close()
                report.update(quality_policy=quality_policy.to_dict(),
                              drama_selection_report=str(selection_path))
                atomic_json(path, report)
                transcode_report, transcode_path = None, None
                # Only after a clean run: a pass that already hit a hard stop
                # (flood/disk/db) should not take on more network work on top.
                if args.command == 'run' and not report.get('stop_reason'):
                    from video_archive.pipeline.batch import drain_transcode, select_ai_candidates
                    selected_rows = filter_approved_rows(
                        outcome_file(args.data, c['project_ref']), c['project_ref'], rows)
                    committed, _ = committed_inventory(db, selected_rows)
                    candidates = select_ai_candidates(selected_rows, committed, limit=None)
                    transcode_path = args.data / ('pipeline-transcode-' + now().replace(':', '-') + '.json')
                    transcode_report = asyncio.run(drain_transcode(
                        db, storage, cache, candidates,
                        concurrency=args.transcode_concurrency, report_path=transcode_path))
                print(json.dumps({'status': report['status'], 'report': str(path),
                                  'drama_selection_report': str(selection_path),
                                  **drama_selection_summary(evaluations),
                                  'cover_counts': report.get('cover_counts', {}),
                                  'dramas': [{'drama_id': item['drama_id'],
                                              'expected': item.get('expected_count', item.get('video_count', 0)),
                                              'verified': item.get('verified_video_count', 0),
                                              'missing': item.get('missing_video_count'),
                                              'missing_ratio': item.get('missing_video_ratio'),
                                              'decision': item.get('delivery_decision'),
                                              'cover_complete': item.get('cover_complete'),
                                              'missing_message_ids': item.get('remaining_video_message_ids', []),
                                              'state': item.get('state')}
                                             for item in report.get('dramas', [])],
                                  'results': [(x['message_id'], x['result'], x.get('error')) for part in report.get('segments', [report]) for x in part['videos']],
                                  'deferred_count': len(deferred),
                                  **({'transcode_counts': transcode_report['counts'],
                                      'transcode_report': str(transcode_path)} if transcode_report else {})}))
                return 0 if (report.get('delivery_accepted') is True
                             or report['status'] == 'all_archived') else 2
            elif args.command == 'ai':
                approved_rows = filter_approved_rows(
                    outcome_file(args.data, c['project_ref']), c['project_ref'], rows)
                if not approved_rows:
                    print(json.dumps({'processed': 0, 'results': [],
                                      'download_gate_excluded_videos': len(rows),
                                      'reason': 'download_completeness_not_approved'}))
                    return 0
                # Pre-flight client-privilege audit intentionally skipped here: anon/
                # authenticated/service_role direct table access is a deliberate,
                # kept decision (not drift), confirmed by the project owner.
                storage.bucket()
                from video_archive.pipeline.ai import build_provider
                from video_archive.pipeline.batch import select_ai_drama_candidates
                provider = build_provider()
                cache = Cache(args.data, project_ref=c['project_ref'])
                committed, _ = committed_inventory(db, approved_rows)
                candidates = select_ai_drama_candidates(approved_rows, committed, limit=args.limit)
                results = []
                for did in candidates:
                    entry = {'drama_id': did}
                    try:
                        run_ai(db, storage, cache, did, provider)
                        entry['result'] = 'ai_succeeded'
                    except PipelineError as exc:
                        entry.update(result='skipped' if str(exc) == 'task_not_claimable' else 'failed',
                                     error=str(exc))
                    except Exception as exc:
                        # Lease.__exit__ already recorded this on the task row (if the
                        # lease was still valid); keep this candidate's failure local
                        # and move on to the next one instead of aborting the whole pass.
                        entry.update(result='failed', error=error_code(exc))
                    results.append(entry)
                print(json.dumps({'processed': len(results), 'results': results}))
            elif args.command == 'transcode-backlog':
                approved_rows = filter_approved_rows(
                    outcome_file(args.data, c['project_ref']), c['project_ref'], rows)
                if not approved_rows:
                    print(json.dumps({'processed': 0, 'results': [],
                                      'download_gate_excluded_videos': len(rows),
                                      'reason': 'download_completeness_not_approved'}))
                    return 0
                # No Telegram connection needed: this only rewraps an already-uploaded
                # object in place. Iterates every archived video and relies on
                # claim() to skip anything without a pending 'transcode' row (not
                # backfilled, already done, or in backoff) -- same shape as 'ai'.
                storage.bucket()
                from video_archive.pipeline.batch import select_ai_candidates
                cache = Cache(args.data, project_ref=c['project_ref'])
                committed, _ = committed_inventory(db, approved_rows)
                candidates = select_ai_candidates(approved_rows, committed, limit=args.limit)
                results = []
                for _, v, r, _ in candidates:
                    entry = {'message_id': r['message_id'], 'video_id': v['id']}
                    try:
                        entry['result'] = transcode_task(db, storage, cache, v['id'])
                    except PipelineError as exc:
                        entry.update(result='skipped' if str(exc) == 'task_not_claimable' else 'failed',
                                     error=str(exc))
                    except Exception as exc:
                        entry.update(result='failed', error=error_code(exc))
                    results.append(entry)
                print(json.dumps({'processed': len(results), 'results': results}))
            elif args.command == 'bucket':
                db.verify_worker()
                print(json.dumps({'videos': storage.bucket(create=True),
                                  'covers': storage.cover_bucket(create=True)}))
            elif args.command == 'import':
                db.verify_worker()
                import_rows(db, rows)
                print(json.dumps({'imported_or_existing': len(rows), 'excluded': excluded}))
            elif args.command == 'samples':
                permissions = db.verify_worker()
                storage.bucket()
                backup_legacy(args.data)
                import_rows(db, samples)
                cache = Cache(args.data, project_ref=c['project_ref'])
                report = {'at': now(), 'project_ref': c['project_ref'], 'bucket': 'drama-videos',
                          'status': 'in_progress', 'samples': [], 'permissions': permissions,
                          'ai_called': False, 'original_three_sample_gate_changed': False,
                          'dashboard_metadata_source': 'administrator_confirmation',
                          'fault_injection': 'not_performed_on_remote'}
                # Keep every run, including partial failure; never erase earlier evidence.
                history = args.data / ('pipeline-remote-' + now().replace(':', '-') + '.json')
                def save_report():
                    atomic_json(history, report)
                    atomic_json(args.data / 'pipeline-remote-acceptance.json', report)
                save_report()
                try:
                    for _, v, r, s in samples:
                        result = archive_existing(db, storage, cache, v, r, s)
                        report['samples'].append({**sample_evidence(db, cache, v, r), 'result': result,
                            'original_retained': local_file(args.data, r, s).is_file()})
                        save_report()
                    report['status'] = 'two_samples_verified'
                except BaseException as exc:
                    report.update(status='incomplete', error=error_code(exc))
                    raise
                finally:
                    save_report()
                print(json.dumps(report['samples']))
        return 0
    except (Exception, KeyboardInterrupt, asyncio.CancelledError) as exc:
        print('STOP: ' + error_code(exc), file=sys.stderr)
        return 130 if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else 2


if __name__ == '__main__':
    def stop(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    raise SystemExit(main())
