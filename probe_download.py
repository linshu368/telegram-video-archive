"""Explicit opt-in, one-video standard API test; NOT a history backfill."""
import argparse
import asyncio
import fcntl
import os
from datetime import datetime

from archive import (BASE, PRIVATE, client_for, confirmed_target, history_page,
                     load_config, private_init, rpc, safe_error)
from core import (StopArchive, atomic_json, classify, disk_check, filename,
                  media_tools, now, record_message, validate_file)


async def probe(client, output, report):
    if not await rpc(client.is_user_authorized) or await rpc(client.is_bot):
        raise StopArchive('authorized_user_session_required')
    peer, source = await confirmed_target(client)
    report['source'] = source
    cutoff = datetime.fromisoformat(report['started_at']).replace(microsecond=0)
    report['cutoff_exclusive_utc'] = cutoff.isoformat()
    offset, upper = 0, None
    selected = None
    # Bounded discovery only. Never claim this sample is exhaustive history.
    for page_number in range(10):
        messages, _ = await history_page(client, peer, offset, upper, cutoff, 100)
        ids = [m.id for m in messages]
        if ids != sorted(set(ids), reverse=True) or (offset and any(i >= offset for i in ids)):
            raise StopArchive('pagination_boundary_error')
        if any(getattr(m, 'date', None) and m.date >= cutoff for m in messages):
            raise StopArchive('message_outside_time_boundary')
        report['pages_checked'] += 1
        report['messages_checked'] += len(messages)
        if not messages:
            break
        if upper is None:
            upper = max(ids)
            report['discovery_upper_id'] = upper
        for message in messages:
            if classify(message)[0] and not getattr(message.media, 'ttl_seconds', None):
                selected = message
                break
        if selected:
            break
        offset = min(ids)
        await asyncio.sleep(0.3)
    if selected is None:
        raise StopArchive('no_video_in_bounded_discovery: not proof that source contains no videos')
    # Refresh the exact message; only this message is attempted, never fallback to another.
    fresh = await rpc(lambda: client.get_messages(peer, ids=selected.id))
    if not fresh or not classify(fresh)[0] or getattr(fresh.media, 'ttl_seconds', None):
        raise StopArchive('selected_message_changed_or_unavailable')
    record = record_message(fresh, source)
    report['selected_message_id'] = fresh.id
    report['expected_size'] = record['expected_size']
    atomic_json(output / 'message.json', record)
    disk_check(output, record['expected_size'] or 0)
    final = output / filename(record)
    partial = final.with_name(final.name + '.part')
    last_progress = 0

    async def progress(current, total):
        nonlocal last_progress
        disk_check(output, max(0, (total or 0) - current))
        if current - last_progress >= 5 * 1024 * 1024 or current == total:
            print(f'bytes={current}/{total}', flush=True)
            last_progress = current

    report.update(result='DOWNLOADING', media_download_attempts=1)
    atomic_json(output / 'report.json', report)
    print(f'source={source["id"]} protected={source["protected"]} '
          f'message={fresh.id} expected_bytes={record["expected_size"]}', flush=True)
    # Intentionally do NOT reject noforwards here. Keep its actual value in metadata.
    # No forwarding, privilege changes, other sources, or alternative access methods.
    with partial.open('wb') as handle:
        await asyncio.wait_for(client.download_media(fresh, file=handle,
                               progress_callback=progress), timeout=600)
        handle.flush()
        os.fsync(handle.fileno())
    result = validate_file(partial, record['expected_size'], decode=True)
    os.replace(partial, final)
    record['download'] = {**result, 'local_path': str(final), 'downloaded_at': now(),
                          'status': 'success'}
    atomic_json(output / 'message.json', record)
    report.update(result='SINGLE_VIDEO_VERIFIED', local_path=str(final), validation=result)


async def run(output, report):
    client = client_for(load_config())
    try:
        await rpc(client.connect)
        await probe(client, output, report)
    finally:
        if client.is_connected():
            await asyncio.wait_for(client.disconnect(), timeout=20)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--attempt-protected', action='store_true', required=True,
                        help='Explicitly attempt one standard API download despite noforwards')
    parser.parse_args()
    private_init()
    with (PRIVATE / 'process.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('Another archive/login process is running.')
            return 2
        output = BASE / 'archive' / ('probe-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
        output.mkdir(parents=True)
        report = {'started_at': now(), 'result': 'STARTING', 'source': None,
                  'pages_checked': 0, 'messages_checked': 0, 'media_download_attempts': 0,
                  'full_backfill_complete': False, 'mode': 'explicit_single_video_api_probe',
                  'limits': 'At most 10 history pages and one media attempt, 600s download timeout.'}
        code = 0
        try:
            media_tools()
            disk_check(output)
            asyncio.run(run(output, report))
        except (KeyboardInterrupt, asyncio.CancelledError):
            report.update(result='INTERRUPTED', failure_reason='interrupted')
            code = 130
        except Exception as exc:
            report.update(result='NOT_VERIFIED', failure_reason=safe_error(exc))
            code = 2
        finally:
            report['finished_at'] = now()
            atomic_json(output / 'report.json', report)
            print(f'report={output / "report.json"} result={report["result"]}', flush=True)
        return code


if __name__ == '__main__':
    raise SystemExit(main())