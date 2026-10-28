#!/usr/bin/env python3
"""Single-source, fixed-boundary Telegram history backfill. No update listener."""
import argparse
import asyncio
import errno
import fcntl
import getpass
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient, errors, functions, types, utils

from video_archive.core import (StopArchive, Store, atomic_json, cover_filename, disk_check, document_of,
                  filename, is_general, media_tools, now, pick_thumb, record_message,
                  validate_cover, validate_file)

from video_archive.paths import BASE
PRIVATE = BASE / '.private'


def _default_entry():
    # Invite links stay in .private (gitignored). A public checkout has no default.
    path = PRIVATE / 'default-entry.txt'
    try:
        return path.read_text().strip()
    except OSError:
        return ''


DEFAULT_ENTRY = _default_entry()
NETWORK_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 1800
RETRIES = 3
MAX_FLOOD_WAIT = 60
on_flood = None  # Optional observer(seconds) set by the bounded batch scheduler.


def safe_error(exc):
    # RPC exception strings may contain requests. Never persist their repr.
    if isinstance(exc, (StopArchive, ValueError)):
        return str(exc)
    return type(exc).__name__


async def rpc(factory, timeout=NETWORK_TIMEOUT):
    for attempt in range(RETRIES):
        try:
            return await asyncio.wait_for(factory(), timeout=timeout)
        except errors.FloodWaitError as exc:
            if on_flood:
                on_flood(exc.seconds)
            if exc.seconds > MAX_FLOOD_WAIT or attempt == RETRIES - 1:
                raise StopArchive(f'flood_wait: {exc.seconds}s; saved, resume later') from None
            await asyncio.sleep(exc.seconds + 1)
        except (OSError, asyncio.TimeoutError, errors.ServerError):
            if attempt == RETRIES - 1:
                raise
            await asyncio.sleep(2 ** attempt)


def private_init():
    os.umask(0o077)
    PRIVATE.mkdir(mode=0o700, exist_ok=True)
    PRIVATE.chmod(0o700)


def load_config(interactive=False):
    path = PRIVATE / 'credentials.json'
    if not path.exists():
        if not interactive or not sys.stdin.isatty():
            raise StopArchive('credentials_missing: run login in your local interactive terminal')
        api_id = int(input('API ID（仅保存在本机）: ').strip())
        api_hash = getpass.getpass('API hash（隐藏输入）: ').strip()
        if api_id <= 0 or not re.fullmatch(r'[0-9a-fA-F]{32}', api_hash):
            raise StopArchive('invalid_api_credentials_format')
        atomic_json(path, {'api_id': api_id, 'api_hash': api_hash})
        path.chmod(0o600)
    config = json.loads(path.read_text())
    if path.stat().st_mode & 0o077:
        raise StopArchive('credentials_permissions: chmod 600 .private/credentials.json')
    return config


def client_for(config):
    return TelegramClient(str(PRIVATE / 'user'), config['api_id'], config['api_hash'],
                          timeout=20, request_retries=0, connection_retries=2,
                          retry_delay=2, auto_reconnect=False, flood_sleep_threshold=0,
                          receive_updates=False, raise_last_call_error=True)


async def login(client):
    if await rpc(client.is_user_authorized):
        print('用户 API session 已登录（未显示账号信息）。')
        return
    if not sys.stdin.isatty():
        raise StopArchive('interactive_login_required')
    phone = getpass.getpass('手机号（国际格式，隐藏输入）: ').strip()
    await rpc(lambda: client.send_code_request(phone))
    for attempt in range(3):
        try:
            code = getpass.getpass('Telegram 验证码（隐藏输入）: ').strip()
            await rpc(lambda: client.sign_in(phone=phone, code=code))
            break
        except errors.SessionPasswordNeededError:
            password = getpass.getpass('两步验证密码（隐藏输入）: ')
            await rpc(lambda: client.sign_in(password=password))
            break
        except errors.PhoneCodeInvalidError:
            if attempt == 2:
                raise StopArchive('invalid_code_attempt_limit') from None
            print('验证码错误，请重新输入。')
    print('登录成功。session 仅保存在 .private/；未读取对话列表或联系人。')


def source_info(entity):
    if not isinstance(entity, (types.Channel, types.Chat)):
        raise StopArchive('target_must_be_group_or_channel_not_user_or_bot')
    if getattr(entity, 'left', False) or getattr(entity, 'kicked', False):
        raise StopArchive('not_a_current_member: join/restore legitimate access manually first')
    kind = ('supergroup' if entity.megagroup else 'channel') if isinstance(entity, types.Channel) else 'basic_group'
    return {'id': utils.get_peer_id(entity), 'bare_id': entity.id, 'name': entity.title or '',
            'username': getattr(entity, 'username', None) or '', 'type': kind,
            'forum': bool(getattr(entity, 'forum', False)),
            'protected': bool(getattr(entity, 'noforwards', False))}


async def resolve(client, entry, confirm):
    entry = entry.strip().rstrip('。')
    invite = re.fullmatch(r'https://t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)', entry)
    if invite:
        result = await rpc(lambda: client(functions.messages.CheckChatInviteRequest(invite.group(1))))
        if not isinstance(result, types.ChatInviteAlready):
            # No ImportChatInviteRequest: even a free join is left to the user.
            raise StopArchive('invite_not_joined_or_not_uniquely_accessible: join manually or provide exact accessible target')
        entity = result.chat
    elif re.fullmatch(r'-?\d+', entry):
        entity = await rpc(lambda: client.get_entity(int(entry)))
    elif re.fullmatch(r'(?:https://t\.me/|@)[A-Za-z][A-Za-z0-9_]+', entry):
        entity = await rpc(lambda: client.get_entity(entry))
    else:
        raise StopArchive('invalid_target_link_or_id')
    source = source_info(entity)
    print(json.dumps(source, ensure_ascii=False, indent=2))
    peer = utils.get_input_peer(entity)
    candidate = {'source': source, 'peer': {'kind': type(peer).__name__, 'id': entity.id,
                                         'access_hash': getattr(peer, 'access_hash', None)}}
    atomic_json(PRIVATE / 'candidate.json', candidate)
    if confirm:
        if not sys.stdin.isatty():
            raise StopArchive('target_confirmation_requires_local_terminal')
        answer = input('确认这是唯一要回填的视频来源，请输入上面的完整 id（其他输入取消）: ').strip()
        if answer != str(source['id']):
            raise StopArchive('target_not_confirmed')
        previous = PRIVATE / 'target.json'
        if previous.exists() and json.loads(previous.read_text())['source']['id'] != source['id']:
            raise StopArchive('existing_whitelist_differs: use a separate project/data directory; not overwritten')
        atomic_json(previous, candidate)
        print('目标已加入单来源白名单。未加入其他群，未发送任何机器人命令。')
    else:
        print('只解析，尚未确认白名单；在本机运行 resolve --confirm。')


async def confirmed_target(client):
    path = PRIVATE / 'target.json'
    if not path.exists():
        raise StopArchive('target_not_confirmed: run resolve --confirm locally')
    target = json.loads(path.read_text())
    data = target['peer']
    if data['kind'] == 'InputPeerChannel':
        peer = types.InputPeerChannel(data['id'], data['access_hash'])
    elif data['kind'] == 'InputPeerChat':
        peer = types.InputPeerChat(data['id'])
    else:
        raise StopArchive('invalid_whitelist_peer_type')
    entity = await rpc(lambda: client.get_entity(peer))
    source = source_info(entity)
    if source['id'] != target['source']['id']:
        raise StopArchive('whitelist_id_mismatch')
    return peer, source


async def history_page(client, peer, offset, upper, cutoff, limit):
    # Use raw pages: a short nonempty page is NOT treated as end of history.
    result = await rpc(lambda: client(functions.messages.GetHistoryRequest(
        peer=peer, offset_id=offset, offset_date=cutoff, add_offset=0, limit=limit,
        max_id=upper + 1 if upper is not None else 0, min_id=0, hash=0)))
    if not hasattr(result, 'messages'):
        raise StopArchive('unexpected_history_response')
    return result.messages, getattr(result, 'topics', [])


async def ensure_task(client, peer, source, store):
    if store.get('source'):
        if store.get('source')['id'] != source['id']:
            raise StopArchive('whitelist_does_not_match_existing_task')
        return
    started = now()
    # Exclusive second-resolution date cutoff avoids including messages arriving
    # during the upper-bound lookup. Same-second messages are deliberately outside.
    cutoff = datetime.fromisoformat(started).replace(microsecond=0)
    first, _ = await history_page(client, peer, 0, None, cutoff, 1)
    upper = max((m.id for m in first), default=0)
    store.start(source, started, upper)
    with store.db:
        store.set('cutoff_exclusive_utc', cutoff.isoformat())


async def scan(client, peer, store, page_size):
    source = store.get('source')
    cutoff = datetime.fromisoformat(store.get('cutoff_exclusive_utc'))
    while not store.get('scan_complete'):
        disk_check(store.root, 10 * 1024 * 1024)
        offset = store.get('cursor')
        page, topics = await history_page(client, peer, offset, store.get('upper_id'), cutoff, page_size)
        ids = [m.id for m in page]
        if ids != sorted(set(ids), reverse=True) or any(i >= offset or i <= 0 for i in ids):
            raise StopArchive('pagination_boundary_or_duplicate_error')
        if any(getattr(m, 'date', None) and m.date >= cutoff for m in page):
            raise StopArchive('server_returned_message_outside_frozen_time_boundary')
        if any(isinstance(m, types.MessageEmpty) for m in page):
            with store.db:
                notes = store.get('limitations', [])
                note = 'API returned MessageEmpty entries: unavailable content cannot be classified'
                if note not in notes:
                    store.set('limitations', notes + [note])
        records = [record_message(m, source) for m in page]
        store.ingest(records, offset, min(ids) if ids else offset, ids, topics)
        count = store.db.execute('SELECT count(*) FROM messages').fetchone()[0]
        print(f'scan messages={count} videos={sum(store.counts().values())} '
              f'known_remaining_bytes={store.remaining_bytes()} cursor={store.get("cursor")} '
              f'free_bytes={disk_check(store.root)}', flush=True)
        await asyncio.sleep(0.3)


def event(state, kind, reason=''):
    state['events'].append({'at': now(), 'event': kind, 'reason': reason})


async def download_one(client, peer, current_source, store, record, state, decode=False, *,
                       timeout_seconds=None, retries=None, preserve_partial=False, metrics=None,
                       check=lambda: None, resume_partial=False):
    async def persist():
        if hasattr(store, 'update_async'):
            await store.update_async(mid, state)
        else:
            store.update(mid, state)

    mid = record['message_id']
    if is_general(record):
        state.update(status='skipped', failure_reason='general_topic_excluded')
        await persist()
        return 'skipped'
    from video_archive.pipeline.cache import archived_local
    if not getattr(store, 'cloud_state_checked', False) and archived_local(store.root, record):
        return 'cloud_archived_skipped'
    final = store.root / 'videos' / filename(record)
    partial = final.with_name(final.name + '.part')
    # Resume in a separate working copy: the original .part is immutable evidence.
    working = partial.with_name(partial.name + '.resume') if resume_partial else partial
    if state['status'] == 'success':
        try:
            check()
            result = await asyncio.to_thread(validate_file, final, record['expected_size'], state['sha256'], decode=decode)
            if state.get('decoded_at') and not result['decoded_at']:
                result['decoded_at'] = state['decoded_at']
            state.update(result)
            event(state, 'verified_skip')
            await persist()
            return 'verified_skipped'
        except (OSError, ValueError, TimeoutError, subprocess.SubprocessError) as exc:
            state.update(status='failed', failure_reason='local_validation: ' + safe_error(exc))
            event(state, 'validation_failed', state['failure_reason'])
            await persist()
    retries = RETRIES if retries is None else retries
    timeout_seconds = DOWNLOAD_TIMEOUT if timeout_seconds is None else timeout_seconds
    for attempt in range(retries):
        state.update(status='downloading', failure_reason='', attempts=state['attempts'] + 1)
        await persist()
        try:
            check()
            if partial.exists():
                if preserve_partial and not resume_partial:
                    raise StopArchive('partial_preserved')
                if not resume_partial:
                    partial.unlink()  # Legacy file-level restart only.
            if final.exists():
                raise StopArchive('existing_local_file_conflict')
            if partial.is_symlink() or working.is_symlink():
                raise StopArchive('partial_path_conflict')
            disk_check(store.root, record['expected_size'] or 0)
            refreshed_source = source_info(await rpc(lambda: client.get_entity(peer)))
            check()
            if refreshed_source['id'] != record['source']['id']:
                raise StopArchive('download_source_whitelist_mismatch')
            fresh = await rpc(lambda: client.get_messages(peer, ids=mid))
            if not fresh or isinstance(fresh, types.MessageEmpty):
                raise ValueError('message_deleted_or_inaccessible_since_scan')
            if getattr(getattr(fresh, 'media', None), 'ttl_seconds', None):
                state.update(status='blocked', failure_reason='expiring_media')
                await persist()
                return 'blocked'
            doc = document_of(fresh)
            if not doc or doc.id != record['media_id'] or doc.size != record['expected_size']:
                raise ValueError('media_changed_since_scan: original_snapshot_not_overwritten')
            # Preserve a credential-free refreshed snapshot without replacing scan text.
            state['download_message_snapshot'] = record_message(fresh, current_source)

            offset = 0
            if resume_partial:
                if partial.exists() and not working.exists():
                    await asyncio.to_thread(shutil.copyfile, partial, working)
                if working.exists():
                    offset = working.stat().st_size
                    if offset > record['expected_size'] or (partial.exists() and offset < partial.stat().st_size):
                        raise StopArchive('partial_size_conflict')
                    if partial.exists():
                        def check_prefix():
                            with partial.open('rb') as original, working.open('rb') as resumed:
                                while True:
                                    block = original.read(1024 * 1024)
                                    if not block:
                                        return
                                    if block != resumed.read(len(block)):
                                        raise StopArchive('partial_prefix_conflict')
                        await asyncio.to_thread(check_prefix)
                if metrics is not None:
                    metrics['resumed_from_bytes'] = offset

            async def progress(current, total):
                check()
                disk_check(store.root, max(0, (total or 0) - current))
                if metrics is not None:
                    metrics['telegram_bytes'] = current - offset
                    metrics['telegram_seconds'] = round(time.monotonic() - transfer_started, 3)

            # Writing an explicit handle guarantees Telethon cannot append a suffix
            # or choose its own path. FileReferenceExpired is retried with fresh msg.
            with working.open('ab' if resume_partial else 'wb') as handle:
                check()
                transfer_started = time.monotonic()
                try:
                    if resume_partial:
                        async def transfer():
                            current = offset
                            stream = client.iter_download(fresh.media, offset=offset,
                                                          request_size=512 * 1024,
                                                          file_size=record['expected_size'])
                            try:
                                async for block in stream:
                                    check()
                                    if current + len(block) > record['expected_size']:
                                        raise StopArchive('partial_size_conflict')
                                    handle.write(block)
                                    current += len(block)
                                    await progress(current, record['expected_size'])
                            finally:
                                await stream.close()
                        if offset < record['expected_size']:
                            await asyncio.wait_for(transfer(), timeout=timeout_seconds)
                    else:
                        await asyncio.wait_for(client.download_media(fresh, file=handle,
                                               progress_callback=progress), timeout=timeout_seconds)
                finally:
                    if metrics is not None:
                        metrics['telegram_seconds'] = round(time.monotonic() - transfer_started, 3)
                handle.flush()
                os.fsync(handle.fileno())
            validation_started = time.monotonic()
            result = await asyncio.to_thread(validate_file, working, record['expected_size'], decode=decode)
            if metrics is not None:
                metrics['download_validation_seconds'] = round(time.monotonic() - validation_started, 3)
            os.replace(working, final)
            state.update(result, status='success', local_path=str(final.relative_to(store.root)),
                         downloaded_at=now(), failure_reason='')
            event(state, 'download_verified')
            await persist()
            return 'downloaded_success'
        except (KeyboardInterrupt, asyncio.CancelledError):
            state.update(status='pending', failure_reason='interrupted_file_will_restart')
            event(state, 'interrupted')
            await persist()
            raise
        except StopArchive as exc:
            state.update(status='pending', failure_reason=safe_error(exc))
            event(state, 'safe_stop', state['failure_reason'])
            await persist()
            raise
        except Exception as exc:
            reason = safe_error(exc)
            state.update(status='failed', failure_reason=reason)
            event(state, 'attempt_failed', reason)
            await persist()
            if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
                raise StopArchive('disk_full: pending file preserved; free disk and resume') from None
            if isinstance(exc, errors.FloodWaitError):
                if on_flood:
                    on_flood(exc.seconds)
                if exc.seconds > MAX_FLOOD_WAIT:
                    raise StopArchive(f'flood_wait: {exc.seconds}s; retry later') from None
                delay = exc.seconds + 1
            else:
                delay = 2 ** attempt
            retryable = isinstance(exc, (OSError, asyncio.TimeoutError, errors.ServerError,
                                         errors.FloodWaitError, errors.FileReferenceExpiredError,
                                         errors.FileReferenceInvalidError))
            if not retryable or attempt == retries - 1:
                return 'failed'
            await asyncio.sleep(delay)
    return 'failed'


async def download_cover(client, peer, store, record, state, *, check=lambda: None,
                         timeout_seconds=120, retries=None, persist=None):
    """Store Telegram's own largest thumbnail as this video's cover.

    Deliberately independent of download_one: a cover failure never touches the
    video's state and vice versa. No ffmpeg, no frame extraction, no video bytes.
    """
    mid = record['message_id']

    async def save():
        if persist:
            await persist(mid, state)
        else:
            store.update_cover(mid, state)

    if is_general(record):
        state.update(status='skipped', failure_reason='general_topic_excluded')
        await save()
        return 'skipped'
    final = store.root / 'covers' / cover_filename(record)
    partial = final.with_name(final.name + '.part')
    if state['status'] == 'success' and final.exists():
        try:
            check()
            result = await asyncio.to_thread(validate_cover, final, state.get('size'))
            if state.get('sha256') and result['sha256'] != state['sha256']:
                raise ValueError('cover_sha256_mismatch')
            state.update(result)
            event(state, 'cover_verified_skip')
            await save()
            return 'verified_skipped'
        except (OSError, ValueError) as exc:
            state.update(status='failed', failure_reason='local_validation: ' + safe_error(exc))
            event(state, 'cover_validation_failed', state['failure_reason'])
            await save()
    retries = RETRIES if retries is None else retries
    for attempt in range(retries):
        state.update(status='downloading', failure_reason='', attempts=state['attempts'] + 1)
        await save()
        try:
            check()
            if partial.is_symlink() or final.is_symlink():
                raise StopArchive('cover_path_conflict')
            partial.unlink(missing_ok=True)
            disk_check(store.root)
            # The cover line refreshes the message itself; it must not wait on the video line.
            fresh = await rpc(lambda: client.get_messages(peer, ids=mid))
            if not fresh or isinstance(fresh, types.MessageEmpty):
                raise ValueError('message_deleted_or_inaccessible_since_scan')
            doc = document_of(fresh)
            if not doc or doc.id != record['media_id']:
                raise ValueError('media_changed_since_scan')
            thumb = pick_thumb(doc)
            if thumb is None:
                # Telegram supplied no fetchable thumbnail. Terminal, not a failure:
                # an ffmpeg frame fallback is explicitly out of scope.
                state.update(status='unavailable', failure_reason='no_thumbnail_available',
                             thumb=None, local_path='', verified_at=now())
                event(state, 'cover_unavailable', 'no_thumbnail_available')
                await save()
                return 'no_thumbnail'
            check()
            with partial.open('wb') as handle:
                await asyncio.wait_for(client.download_media(fresh, file=handle, thumb=thumb['index']),
                                       timeout=timeout_seconds)
                handle.flush()
                os.fsync(handle.fileno())
            expected = None if thumb.get('progressive') else thumb['size']
            result = await asyncio.to_thread(validate_cover, partial, expected)
            os.replace(partial, final)
            state.update(result, status='success', local_path=str(final.relative_to(store.root)),
                         thumb=thumb, downloaded_at=now(), failure_reason='')
            event(state, 'cover_downloaded')
            await save()
            return 'downloaded_success'
        except (KeyboardInterrupt, asyncio.CancelledError):
            state.update(status='pending', failure_reason='interrupted_cover_will_restart')
            event(state, 'cover_interrupted')
            await save()
            raise
        except StopArchive as exc:
            state.update(status='pending', failure_reason=safe_error(exc))
            event(state, 'cover_safe_stop', state['failure_reason'])
            await save()
            raise
        except Exception as exc:
            partial.unlink(missing_ok=True)
            reason = safe_error(exc)
            state.update(status='failed', failure_reason=reason)
            event(state, 'cover_attempt_failed', reason)
            await save()
            if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
                raise StopArchive('disk_full: pending file preserved; free disk and resume') from None
            if isinstance(exc, errors.FloodWaitError):
                if on_flood:
                    on_flood(exc.seconds)
                if exc.seconds > MAX_FLOOD_WAIT:
                    raise StopArchive(f'flood_wait: {exc.seconds}s; retry later') from None
                delay = exc.seconds + 1
            else:
                delay = 2 ** attempt
            retryable = isinstance(exc, (OSError, asyncio.TimeoutError, errors.ServerError,
                                         errors.FloodWaitError, errors.FileReferenceExpiredError,
                                         errors.FileReferenceInvalidError))
            if not retryable or attempt == retries - 1:
                return 'failed'
            await asyncio.sleep(delay)
    return 'failed'


def choose_sample(store, count):
    chosen = []
    rows = store.videos()
    # Prefer an attachment without video attributes, empty optional fields, and
    # an album/reply. If the source lacks a category, do not fabricate examples.
    predicates = [lambda r: 'DocumentAttributeVideo' not in r['video_evidence'],
                  lambda r: not r['title'] and not r['tags'] and not r['description'],
                  lambda r: bool(r['album_id'] or r['reply'])]
    for predicate in predicates:
        match = next((r['message_id'] for r, s in rows if s['status'] not in ('blocked', 'skipped')
                      and r['message_id'] not in chosen and predicate(r)), None)
        if match is not None and len(chosen) < count:
            chosen.append(match)
    for r, s in rows:
        if len(chosen) >= count:
            break
        if s['status'] not in ('blocked', 'skipped') and r['message_id'] not in chosen:
            chosen.append(r['message_id'])
    return chosen


async def download_set(client, peer, source, store, operation, ids=None, only_failed=False, decode=False, max_concurrent=2):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def worker(record, state):
        async with semaphore:
            outcome = await download_one(client, peer, source, store, record, state, decode=decode)
            return record['message_id'], outcome

    items = []
    for record, state in store.videos():
        if state['status'] in ('blocked', 'skipped'):
            continue
        if ids is not None and record['message_id'] not in ids:
            continue
        if only_failed and state['status'] != 'failed':
            continue
        items.append((record, state))

    if not items:
        return

    # Run tasks with concurrency limit and update operation state safely
    pending_tasks = [asyncio.create_task(worker(r, s)) for r, s in items]
    try:
        for coro in asyncio.as_completed(pending_tasks):
            mid, outcome = await coro
            operation[outcome] = operation.get(outcome, 0) + 1
            with store.db:
                store.set('last_operation', operation)
            print(f'download message={mid} outcome={outcome}', flush=True)
    finally:
        # Drain workers before disconnecting the client or closing SQLite.
        for task in pending_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending_tasks, return_exceptions=True)



def verify(store):
    failures = []
    count = 0
    sampled = store.get('sample_ids', [])
    for record, state in store.videos():
        if state['status'] != 'success':
            continue
        from video_archive.pipeline.cache import archived_local
        if archived_local(store.root, record) and not (store.root / 'videos' / filename(record)).exists():
            # Preserve legacy sample gate and local evidence; cloud verification is
            # reported separately by pipeline.py, never invented as a local decode.
            continue
        try:
            expected_path = store.root / 'videos' / filename(record)
            if state['local_path'] != str(expected_path.relative_to(store.root)):
                raise ValueError('local_path_identity_mismatch')
            result = validate_file(expected_path, record['expected_size'], state['sha256'],
                                   decode=record['message_id'] in sampled)
            if state.get('decoded_at') and not result['decoded_at']:
                result['decoded_at'] = state['decoded_at']
            state.update(result)
            count += 1
        except (OSError, ValueError, TimeoutError, subprocess.SubprocessError) as exc:
            state.update(status='failed', failure_reason='verify: ' + safe_error(exc))
            event(state, 'verification_failed', state['failure_reason'])
            failures.append(record['message_id'])
        store.update(record['message_id'], state)
    sampled_states = {r['message_id']: s for r, s in store.videos() if r['message_id'] in sampled}
    sample_ok = bool(sampled) and all(s['status'] == 'success' and s.get('decoded_at')
                                    for s in sampled_states.values()) and len(sampled_states) == len(sampled)
    with store.db:
        store.set('sample_passed', sample_ok)
        store.set('last_verification', {'at': now(), 'ok': not failures, 'success_count': count,
                                        'failed_ids': failures, 'sample_fully_decoded': sample_ok})
    return not failures


async def execute(args, store):
    if args.command == 'scan':
        # Refresh evidence must not survive a failed login or history scan.
        with store.db:
            store.set('source_topic_inventory_complete', False)
            store.set('source_topic_ids', [])
    if args.command in ('verify', 'report'):
        if args.command == 'verify':
            media_tools()
            verify(store)
        store.export()
        report = store.report()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report['result'] == 'COMPLETE_VERIFIED' else 2
    config = load_config(interactive=args.command == 'login')
    client = client_for(config)
    await rpc(client.connect)
    try:
        if args.command == 'login':
            await login(client)
            return 0
        if not await rpc(client.is_user_authorized):
            raise StopArchive('login_required: run login in local terminal')
        if await rpc(client.is_bot):
            raise StopArchive('user_account_required_not_bot')
        if args.command == 'resolve':
            await resolve(client, args.entry, args.confirm)
            return 0
        if args.command != 'scan':
            media_tools()
        disk_check(store.root)
        peer, source = await confirmed_target(client)
        with store.db:
            store.set('preflight', {'checked_at': now(), 'source': source,
                                   'user_session_authorized': True,
                                   'download_allowed': args.command != 'scan',
                                   'metadata_only': args.command == 'scan'})
        # Note: Content protection flag (source['protected']) is acknowledged and recorded,
        # but per explicit user opt-in and verified probe success, backfill/download is permitted.
        if args.command in ('resume', 'retry') and not store.get('source'):
            raise StopArchive('no_existing_backfill: run sample or backfill first')
        await ensure_task(client, peer, source, store)
        operation = {'command': args.command, 'started_at': now(), 'downloaded_success': 0,
                     'verified_skipped': 0, 'failed': 0, 'blocked': 0}
        with store.db:
            store.set('stop_reason', '')
            store.set('last_operation', operation)
            if args.command != 'scan':
                store.set('last_verification', {})
        await scan(client, peer, store, args.page_size)
        if args.command == 'scan':
            # A complete metadata inventory is the boundary for whole-drama
            # filtering. Do not reset download/cover states or run media tools.
            if not store.get('scan_complete'):
                raise StopArchive('metadata_scan_incomplete')
            from video_archive.screening.inventory import collect_source_inventory
            source_inventory = await collect_source_inventory(
                client, peer, source, store, page_size=args.page_size)
            operation.update(finished_at=now(), metadata_scan_complete=True,
                             source_topic_inventory_complete=True,
                             source_topics=len(source_inventory['completed_topic_ids']),
                             media_downloaded=False)
            with store.db:
                store.set('last_operation', operation)
            print(json.dumps({'status': 'metadata_scan_complete',
                              'upper_message_id_inclusive': store.get('upper_id'),
                              'identified_videos': sum(store.counts().values()),
                              'source_topics': len(source_inventory['completed_topic_ids']),
                              'downloaded_success': 0}))
            return 0
        store.prepare_downloads()
        if args.command == 'retry':
            await download_set(client, peer, source, store, operation, only_failed=True)
        else:
            if not store.get('sample_passed') or args.command == 'sample':
                ids = store.get('sample_ids') or choose_sample(store, args.sample_count)
                with store.db:
                    store.set('sample_ids', ids)
                await download_set(client, peer, source, store, operation, ids=ids, decode=True)
                verify(store)
                if any(s['status'] != 'skipped' for _, s in store.videos()) and not store.get('sample_passed'):
                    raise StopArchive('sample_not_passed: full download not started')
            if args.command != 'sample':
                disk_check(store.root, store.remaining_bytes())
                # Sample successes count only once per operation, not again as skips.
                all_ids = [r['message_id'] for r, _ in store.videos()]
                just_sampled = ids if 'ids' in locals() else []
                await download_set(client, peer, source, store, operation,
                                   ids=[i for i in all_ids if i not in just_sampled])
        verify(store)
        operation['finished_at'] = now()
        with store.db:
            store.set('last_operation', operation)
        return 0
    finally:
        if client.is_connected():
            await asyncio.wait_for(client.disconnect(), timeout=20)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data', type=Path, default=BASE / 'archive', help='absolute local archive directory')
    subs = p.add_subparsers(dest='command', required=True)
    subs.add_parser('login', help='local interactive credentials, code and 2FA')
    r = subs.add_parser('resolve', help='resolve exact target without joining')
    r.add_argument('--entry', default=DEFAULT_ENTRY)
    r.add_argument('--confirm', action='store_true', help='locally type the resolved ID to whitelist')
    metadata = subs.add_parser('scan', help='scan metadata only for whole-drama planning; no media downloads')
    metadata.add_argument('--page-size', type=int, choices=range(1, 101), default=100, metavar='1..100')
    for name in ('sample', 'backfill', 'resume', 'retry'):
        sub = subs.add_parser(name)
        sub.add_argument('--page-size', type=int, choices=range(1, 101), default=100, metavar='1..100')
        sub.add_argument('--sample-count', type=int, choices=range(1, 11), default=3, metavar='1..10')
    subs.add_parser('verify', help='offline size/hash/probe verification and sampled full decode')
    subs.add_parser('report', help='export current state; does not claim to freshly verify files')
    return p


def main():
    args = parser().parse_args()
    private_init()
    if not args.data.is_absolute():
        raise SystemExit('--data must be an absolute path')
    # One process per session/project; avoids competing cursors and SQLite sessions.
    with (PRIVATE / 'process.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('Another archive/login process is running.', file=sys.stderr)
            return 2
        store = Store(args.data)
        code = 0
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            task = loop.create_task(execute(args, store))
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, task.cancel)
            try:
                code = loop.run_until_complete(task)
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
        except (KeyboardInterrupt, asyncio.CancelledError):
            with store.db:
                store.set('stop_reason', 'interrupted: resume same fixed-boundary backfill')
            print('Interrupted; progress saved.', file=sys.stderr)
            code = 130
        except Exception as exc:
            reason = safe_error(exc)
            with store.db:
                store.set('stop_reason', reason)
            print(f'STOP: {reason}', file=sys.stderr)
            code = 2
        finally:
            try:
                store.export()
                report = store.report()
                print(f'report={store.root / "report.json"} result={report["result"]}')
                if args.command in ('backfill', 'resume', 'retry') and report['result'] != 'COMPLETE_VERIFIED':
                    code = code or 2
            except OSError:
                print('Export failed (possibly disk full); state.sqlite3 remains the recovery source.', file=sys.stderr)
                code = 2
            store.db.close()
        return code


if __name__ == '__main__':
    raise SystemExit(main())
