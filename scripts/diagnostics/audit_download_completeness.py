"""Random whole-drama download audit, isolated from production and cloud storage."""
import argparse
import asyncio
from collections import Counter, defaultdict
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import random
import shutil
import signal
import sqlite3
import sys

from telethon import errors

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from video_archive.core import StopArchive, Store, atomic_json, filename, media_tools, now, validate_file
from video_archive.paths import BASE
from video_archive.screening.inventory import _BoundedRepliesClient, _validate_records
from video_archive.telegram.archive import (
    client_for, confirmed_target, download_one, load_config, private_init, rpc, safe_error, session_paths,
)
from video_archive.telegram.topics import Catalog, collect_replies


TRANSPORT_REASONS = frozenset({
    'ConnectionError', 'ConnectionResetError', 'ConnectionAbortedError',
    'ConnectionRefusedError', 'BrokenPipeError', 'IncompleteReadError',
    'TimeoutError', 'OSError', 'ServerError', 'RpcCallFailError', 'TimedOutError',
})


def transport_failure(reason):
    return reason in TRANSPORT_REASONS


def effective_attempts(entry, state):
    """Preserve raw attempts; a dead connection cannot exhaust the media budget."""
    history = sum(not h.get('infrastructure_error', transport_failure(h.get('failure_reason')))
                  for h in entry.get('history', []))
    events = sum(e.get('event') in ('attempt_failed', 'download_verified')
                 and not transport_failure(e.get('reason')) for e in state.get('events', []))
    return max(history, events)


def resume_existing(output, report, store):
    """Validate reused files and reopen only transport-invalidated decisions."""
    rows = {r['message_id']: (r, s) for r, s in store.videos()}
    if not rows and not report.get('dramas'):
        return
    snapshot = output / 'recovery-history' / now().replace(':', '-')
    snapshot.mkdir(parents=True)
    for name in ('report.json', 'summary.json', 'selection.json'):
        path = output / name
        if path.exists():
            shutil.copy2(path, snapshot / name)
    with sqlite3.connect(snapshot / 'state.sqlite3') as backup:
        store.db.backup(backup)
    # Validate every reused success, including already-finalized whole dramas.
    # No download API or file mutation occurs in this phase.
    verified = {}
    for mid, (record, state) in rows.items():
        if state['status'] == 'success':
            try:
                verified[mid] = validate_file(output / 'videos' / filename(record),
                                            record['expected_size'], state['sha256'])
            except Exception as exc:
                raise StopArchive(f'resume_file_not_verified: message={mid}; {safe_error(exc)}') from None
    reopened_videos = 0
    for mid, (_, state) in rows.items():
        if mid in verified:
            state.update(verified[mid])
            state['events'].append({'at': now(), 'event': 'audit_resume_verified'})
            store.update(mid, state)
        elif transport_failure(state.get('failure_reason')) or state['status'] == 'downloading':
            # Keep cumulative attempt numbers and every original failure event.
            state['status'] = 'pending'
            state['events'].append({'at': now(), 'event': 'audit_resume_pending',
                                    'reason': state.get('failure_reason', '')})
            store.update(mid, state)
            reopened_videos += 1
    reopened_metadata = 0
    for drama in report.get('dramas', {}).values():
        for video in drama.get('videos', []):
            for history in video.get('history', []):
                history['infrastructure_error'] = transport_failure(history.get('failure_reason'))
            if video['message_id'] in verified:
                video.update(verified[video['message_id']], success=True, failure_reason='')
                if not video['history']:
                    video['history'].append({'at': now(), 'success': True,
                                            'result': 'recovered_verified_sqlite_receipt'})
        if drama['status'] == 'metadata_failed' and transport_failure(drama.get('error')):
            drama.setdefault('metadata_failure_history', []).append(
                {'at': now(), 'error': drama.pop('error'), 'infrastructure_error': True})
            drama['status'] = 'queued'
            reopened_metadata += 1
        elif drama['status'] == 'finished' and any(
                not video['success'] and rows[video['message_id']][1]['status'] == 'pending'
                for video in drama.get('videos', [])):
            keys = ('finished_at', 'outcome', 'delivery_decision', 'success_count',
                    'failure_count', 'first_pass_failed_count')
            drama.setdefault('previous_decisions', []).append(
                {**{key: drama.pop(key) for key in keys if key in drama},
                 'invalidated_reason': 'transport_interruption_requires_resume'})
            drama['status'] = 'metadata_ready'
    partials = [path for path in (output / 'videos').iterdir()
                if path.name.endswith(('.part', '.resume'))]
    report['resume'] = {'at': now(), 'snapshot': str(snapshot),
                        'verified_existing_video_count': len(verified),
                        'requeued_video_count': reopened_videos,
                        'requeued_metadata_drama_count': reopened_metadata,
                        'retained_partial_count': len(partials),
                        'retained_partial_bytes': sum(p.stat().st_size for p in partials)}
    report.setdefault('resume_history', []).append(dict(report['resume']))
    report.pop('finished_at', None)


def prepare(source_root, output, count, seed):
    """Freeze the draw before observing fresh failures; never replace bad samples."""
    plan_path = output / 'selection.json'
    if plan_path.exists():
        return json.loads(plan_path.read_text())
    connection = sqlite3.connect(f'{(source_root / "state.sqlite3").as_uri()}?mode=ro', uri=True)
    try:
        task = {key: json.loads(value) for key, value in connection.execute('SELECT * FROM task')}
        if not task.get('scan_complete') or not task.get('cutoff_exclusive_utc'):
            raise StopArchive('completed_source_history_required')
        groups = defaultdict(list)
        for raw, in connection.execute('SELECT data FROM messages ORDER BY id'):
            record = json.loads(raw)
            if record.get('video_evidence') and record.get('topic_id') not in (None, 1):
                groups[record['topic_id']].append(record)
        titles = {tid: json.loads(raw).get('title') for tid, raw in connection.execute('SELECT * FROM topics')}
    finally:
        connection.close()
    if count > len(groups):
        raise StopArchive('sample_larger_than_population')
    selected = random.Random(seed).sample(sorted(groups), count)
    plan = {'created_at': now(), 'source': task['source'], 'seed': seed,
            'population_drama_count': len(groups), 'requested_drama_count': count,
            'source_boundary': {key: task[key] for key in ('upper_id', 'cutoff_exclusive_utc')},
            'selection': 'uniform_without_replacement_from_topics_with_videos_in_frozen_history',
            'quality_or_aspect_filters': False, 'max_attempts_per_video': 5,
            'dramas': [{'topic_id': tid, 'topic_title': titles.get(tid), 'records': groups[tid]}
                       for tid in selected]}
    plan['expected_video_count'] = sum(len(d['records']) for d in plan['dramas'])
    plan['expected_bytes'] = sum(r.get('expected_size') or 0 for d in plan['dramas'] for r in d['records'])
    atomic_json(plan_path, plan)
    return plan


def delivery(success, total):
    if not total:
        return 'unassessed'
    missing = total - success
    return ('complete' if not missing else 'accepted_with_gaps' if missing * 10 < total
            else 'rejected_incomplete')


def wilson(success, total):
    if not total:
        return None
    z = 1.959963984540054
    p = success / total
    divisor = 1 + z * z / total
    center = (p + z * z / (2 * total)) / divisor
    half = z * ((p * (1 - p) / total + z * z / (4 * total * total)) ** .5) / divisor
    return [max(0, center - half), min(1, center + half)]


def summarize(plan, results):
    finished = [r for r in results.values() if r.get('status') == 'finished']
    counts = Counter(r['outcome'] for r in finished)
    decisions = Counter(r['delivery_decision'] for r in finished)
    videos = [v for r in finished for v in r['videos']]
    partial = counts['partial_failure']
    n = len(finished)
    return {'population_drama_count': plan['population_drama_count'],
            'requested_drama_count': plan['requested_drama_count'],
            'finished_drama_count': n,
            'unassessed_drama_count': sum(r.get('status') == 'metadata_failed' for r in results.values()),
            'drama_outcomes': dict(counts), 'delivery_decisions': dict(decisions),
            'partial_failure_drama_ratio': partial / n if n else None,
            'partial_failure_drama_95pct_wilson_interval': wilson(partial, n),
            'rejected_drama_ratio': decisions['rejected_incomplete'] / n if n else None,
            'rejected_drama_95pct_wilson_interval': wilson(decisions['rejected_incomplete'], n),
            'partial_failure_and_rejected_drama_count': sum(
                r['outcome'] == 'partial_failure' and r['delivery_decision'] == 'rejected_incomplete'
                for r in finished),
            'first_pass_incomplete_drama_count': sum(r['first_pass_failed_count'] > 0 for r in finished),
            'evaluated_video_count': len(videos),
            'first_pass_failed_video_count': sum(not v['history'][0]['success'] for v in videos),
            'final_failed_video_count': sum(not v['success'] for v in videos),
            'final_failed_video_ratio': sum(not v['success'] for v in videos) / len(videos) if videos else None,
            'recovered_on_retry_video_count': sum(v['success'] and not v['history'][0]['success'] for v in videos),
            'first_pass_transport_interrupted_video_count': sum(
                transport_failure(v['history'][0].get('failure_reason')) for v in videos),
            'transport_interruption_event_count': sum(
                transport_failure(h.get('failure_reason'))
                for r in results.values() for v in r.get('videos', []) for h in v.get('history', [])),
            'verified_bytes': sum(v.get('actual_size') or 0 for v in videos if v['success']),
            'final_failure_reasons': dict(Counter(v.get('failure_reason') or 'unknown'
                                                for v in videos if not v['success']))}


def save(output, plan, report):
    report['updated_at'] = now()
    report['summary'] = summarize(plan, report['dramas'])
    atomic_json(output / 'report.json', report)
    atomic_json(output / 'summary.json', report['summary'])
    for name, rows in [('dramas', report['dramas'].values()),
                       ('videos', (dict(v, topic_id=r['topic_id'])
                                   for r in report['dramas'].values() for v in r.get('videos', [])))]:
        temporary = output / (name + '.jsonl.tmp')
        with temporary.open('w') as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + '\n')
        os.replace(temporary, output / (name + '.jsonl'))
    s = report['summary']
    lines = ['下载有效性抽样审计', f'状态：{report["status"]}',
             f'更新时间（UTC）：{report["updated_at"]}',
             f'总体 {s["population_drama_count"]} 部，随机抽样 {s["requested_drama_count"]} 部；'
             f'已完成 {s["finished_drama_count"]} 部，清单核查失败 {s["unassessed_drama_count"]} 部。',
             '不按清晰度、长宽或旧下载状态筛选。每个视频最多 5 次有效尝试；网络中断另存历史。',
             '本次审计续跑时重新校验已成功文件后跳过；不复用其他任务的成功记录。',
             '连接自动恢复有次数上限，恢复失败就保存进度停止，不跳过剩余样本。',
             '单文件验证：完整字节大小、SHA-256、ffprobe 视频流；不声称完整逐帧解码。',
             '整剧拒绝是仅按视频下载缺失率 >=10% 的模拟判断；本实验不实际删除整剧记录。',
             '来源范围：原扫描冻结边界内可访问视频；无法证明源曾发布但已删除/不可见的视频数。',
             f'视频保留方式：{report["media_retention"]}', '']
    if s['finished_drama_count']:
        lines += [f'全部成功 / 部分失败 / 全部失败：{s["drama_outcomes"]}',
                  f'部分失败剧占比：{s["partial_failure_drama_ratio"]:.2%}',
                  f'按 10% 缺失阈值的整剧拒绝率：{s["rejected_drama_ratio"]:.2%}',
                  f'部分成功但整剧被拒绝：{s["partial_failure_and_rejected_drama_count"]} 部',
                  f'已判定视频 {s["evaluated_video_count"]} 个，首轮失败 '
                  f'{s["first_pass_failed_video_count"]} 个，重试后失败 {s["final_failed_video_count"]} 个。',
                  f'首轮失败中连接中断 {s["first_pass_transport_interrupted_video_count"]} 个；'
                  f'累计连接中断事件 {s["transport_interruption_event_count"]} 次，原始历史保留。',
                  '95% Wilson 区间见 summary.json；视频共享网络和剧，不能当作独立剧样本。']
    if report.get('stop_reason'):
        lines += [f'停止原因：{report["stop_reason"]}']
    (output / 'report.txt').write_text('\n'.join(lines) + '\n')


async def execute(args, output, plan, report, store):
    session_name = getattr(args, 'session', 'default')
    # Telethon's reconnect budget is already bounded to two connection retries.
    # Other entry points retain their existing default connection policy.
    client = client_for(load_config(), session_name=session_name, auto_reconnect=True)
    metadata_root = output / 'source-metadata'
    metadata_root.mkdir(exist_ok=True)
    catalog = Catalog(metadata_root)
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, current.cancel)
    try:
        await rpc(client.connect)
        if not await rpc(client.is_user_authorized) or await rpc(client.is_bot):
            raise StopArchive('authorized_user_session_required')
        peer, source = await confirmed_target(client)
        if source['id'] != plan['source']['id']:
            raise StopArchive('audit_source_mismatch')
        report['connected_at'] = now()
        report['connection_policy'] = {'auto_reconnect': True, 'connection_retries': 2,
                                        'unrecovered_transport_error': 'stop_and_preserve_progress'}
        report['status'] = 'collecting_metadata' if args.metadata_only else 'running'
        save(output, plan, report)
        boundary = plan['source_boundary']
        cutoff = datetime.fromisoformat(boundary['cutoff_exclusive_utc'])
        bounded = _BoundedRepliesClient(client, boundary['upper_id'])
        semaphore = asyncio.Semaphore(args.concurrency)
        for index, selected in enumerate(plan['dramas'], 1):
            tid = selected['topic_id']
            drama = report['dramas'].setdefault(str(tid), {'topic_id': tid,
                'topic_title': selected['topic_title'], 'status': 'queued', 'videos': []})
            if drama['status'] in ('finished', 'metadata_failed'):
                continue
            report['active_topic_id'] = tid
            report['active_drama_number'] = index
            save(output, plan, report)
            if not drama.get('metadata_complete'):
                try:
                    # A previous partial traversal may contain records omitted by
                    # a later successful traversal. Keep page audit, clear only
                    # this topic's derived message cache before trying again.
                    with catalog.db:
                        old = [(sid, mid) for sid, mid, raw in catalog.db.execute('SELECT * FROM messages')
                               if json.loads(raw).get('requested_topic_id') == tid]
                        catalog.db.executemany('DELETE FROM messages WHERE source_id=? AND message_id=?', old)
                    replies = await collect_replies(bounded, peer, source, catalog, tid, cutoff,
                                                    max_pages=1000)
                    fresh = [json.loads(row[0]) for row in catalog.db.execute('SELECT data FROM messages')]
                    fresh = [r for r in fresh if r.get('requested_topic_id') == tid]
                    _validate_records(fresh, replies, source['id'], tid, boundary['upper_id'], cutoff)
                except StopArchive as exc:
                    if not str(exc).startswith(('source_topic_', 'replies_pagination_',
                                                'replies_outside_', 'selected_topic_page_')):
                        raise
                    drama.update(status='metadata_failed', error=safe_error(exc))
                    save(output, plan, report)
                    continue
                except Exception as exc:
                    if (transport_failure(safe_error(exc))
                            or isinstance(exc, (ConnectionError, asyncio.IncompleteReadError,
                                                asyncio.TimeoutError, errors.ServerError))):
                        drama.update(status='queued', last_transport_error=safe_error(exc))
                        save(output, plan, report)
                        raise StopArchive('connection_recovery_exhausted_during_metadata: '
                                          + safe_error(exc)) from None
                    drama.update(status='metadata_failed', error=safe_error(exc))
                    save(output, plan, report)
                    continue
                # Retain original members even if the current API no longer lists them.
                expected = {r['message_id']: r for r in selected['records']}
                for record in fresh:
                    if record.get('video_evidence'):
                        expected.setdefault(record['message_id'], record)
                store.ingest_topic_records(list(expected.values()))
                drama.update(metadata_complete=True, replies=replies,
                             expected_message_ids=sorted(expected), expected_count=len(expected),
                             original_members_not_in_fresh_listing=sorted(set(expected) - {
                                 r['message_id'] for r in fresh if r.get('video_evidence')}),
                             status='metadata_ready')
                save(output, plan, report)
            if args.metadata_only:
                print(f'metadata {index}/{len(plan["dramas"])} topic={tid} videos={drama["expected_count"]}', flush=True)
                continue
            by_mid = {r['message_id']: (r, s) for r, s in store.videos()}
            entries = {v['message_id']: v for v in drama['videos']}
            for mid in drama['expected_message_ids']:
                entries.setdefault(mid, {'message_id': mid, 'media_id': by_mid[mid][0]['media_id'],
                    'expected_size': by_mid[mid][0]['expected_size'], 'success': False, 'history': []})
            drama['videos'] = list(entries.values())
            drama['status'] = 'downloading'
            save(output, plan, report)

            async def attempt(mid):
                async with semaphore:
                    record, state = by_mid[mid]
                    entry = entries[mid]
                    # A verified result survives a crash between SQLite and report writes.
                    if state['status'] == 'success':
                        entry.update(success=True, **{key: state.get(key) for key in (
                            'actual_size', 'sha256', 'verified_at', 'probe')})
                        if not entry['history']:
                            entry['history'].append({'at': state.get('verified_at'), 'success': True,
                                                     'result': 'recovered_verified_sqlite_receipt'})
                        save(output, plan, report)
                        return
                    metrics = {}
                    started = now()
                    result = await download_one(client, peer, source, store, record, state,
                                                retries=1, timeout_seconds=1800, metrics=metrics,
                                                resume_partial=True)
                    success = result == 'downloaded_success'
                    infrastructure_error = transport_failure(state.get('failure_reason'))
                    entry['history'].append({'started_at': started, 'finished_at': now(),
                        'attempt': state['attempts'], 'result': result, 'success': success,
                        'infrastructure_error': infrastructure_error,
                        'failure_reason': state.get('failure_reason'), 'metrics': metrics})
                    entry.update(success=success, failure_reason=state.get('failure_reason'),
                                 **{key: state.get(key) for key in ('actual_size', 'sha256', 'verified_at', 'probe')})
                    save(output, plan, report)
                    if infrastructure_error:
                        state['status'] = 'pending'
                        store.update(mid, state)
                        raise StopArchive('connection_recovery_exhausted_during_download: '
                                          + state['failure_reason'])
                    print(f'drama={index}/{len(plan["dramas"])} topic={tid} message={mid} '
                          f'attempt={state["attempts"]} result={result}', flush=True)

            for round_number in range(5):
                pending = [mid for mid, entry in entries.items()
                           if not entry['success'] and effective_attempts(entry, by_mid[mid][1]) < 5
                           and by_mid[mid][1]['status'] != 'blocked']
                if not pending:
                    break
                report['active_retry_round'] = round_number
                tasks = [asyncio.create_task(attempt(mid)) for mid in pending]
                try:
                    await asyncio.gather(*tasks)
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                if any(not entries[mid]['success'] for mid in pending) and round_number < 4:
                    await asyncio.sleep(2 ** round_number)
            success = sum(v['success'] for v in drama['videos'])
            total = len(drama['videos'])
            drama.update(status='finished', finished_at=now(), success_count=success,
                         failure_count=total - success,
                         first_pass_failed_count=sum(not v['history'][0]['success'] for v in drama['videos']),
                         outcome='all_success' if success == total else 'partial_failure' if success else 'all_failed',
                         delivery_decision=delivery(success, total))
            save(output, plan, report)
            print(f'finished_drama={index} success={success}/{total} decision={drama["delivery_decision"]}', flush=True)
        unassessed = sum(d['status'] == 'metadata_failed' for d in report['dramas'].values())
        report['status'] = ('incomplete' if unassessed else
                            'metadata_ready' if args.metadata_only else 'finished')
        if unassessed:
            report['stop_reason'] = f'metadata_unassessed: {unassessed} dramas'
        report['finished_at'] = now()
        report.pop('active_topic_id', None)
    finally:
        catalog.db.close()
        if client.is_connected():
            await asyncio.wait_for(client.disconnect(), timeout=20)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-data', type=Path, default=BASE / 'archive')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--seed', type=int, default=20261028)
    parser.add_argument('--session', default='default', help='independent named Telegram session')
    parser.add_argument('--concurrency', type=int, choices=range(1, 5), default=4)
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--metadata-only', action='store_true')
    args = parser.parse_args()
    private_init()
    output = args.output.expanduser().resolve()
    if output == args.source_data.expanduser().resolve() or args.count < 1:
        parser.error('use a separate audit output directory and a positive count')
    output.mkdir(parents=True, exist_ok=True)
    # Offline planning is safe while another job holds the Telegram session.
    plan = prepare(args.source_data.expanduser().resolve(), output, args.count, args.seed)
    report_path = output / 'report.json'
    report = json.loads(report_path.read_text()) if report_path.exists() else {
        'started_at': now(), 'status': 'prepared', 'dramas': {},
        'scope': 'local_download_only_no_cloud_no_ai_no_quality_filters',
        'media_retention': 'keep_all_stop_when_disk_insufficient'}
    if args.prepare_only:
        print(json.dumps({k: plan[k] for k in ('requested_drama_count', 'expected_video_count', 'expected_bytes')}))
        return 0
    session_path, lock_path = session_paths(args.session)
    report['telegram_session'] = {'name': args.session, 'path': str(session_path) + '.session',
                                  'lock_path': str(lock_path)}
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            report['status'] = 'waiting_for_telegram_session'
            save(output, plan, report)
            print('Waiting for the existing Telegram job; downloads will start automatically after it exits.', flush=True)
            fcntl.flock(lock, fcntl.LOCK_EX)
        store = Store(output)
        store.cloud_state_checked = True
        code = 0
        try:
            media_tools()
            resume_existing(output, report, store)
            report.pop('stop_reason', None)
            report['status'] = 'starting'
            save(output, plan, report)
            print('resume=' + json.dumps(report.get('resume', {}), ensure_ascii=False), flush=True)
            asyncio.run(execute(args, output, plan, report, store))
            if report['status'] == 'incomplete':
                code = 2
        except (KeyboardInterrupt, asyncio.CancelledError):
            report.update(status='interrupted', stop_reason='interrupted_resume_same_output')
            code = 130
        except Exception as exc:
            report.update(status='stopped', stop_reason=safe_error(exc))
            code = 2
        finally:
            save(output, plan, report)
            store.db.close()
            print(f'status={report["status"]} report={report_path}', flush=True)
        return code


if __name__ == '__main__':
    raise SystemExit(main())
