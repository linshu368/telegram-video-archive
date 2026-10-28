"""Read-only Telegram forum inventory; separate from media/backfill state."""
import argparse
import asyncio
import csv
import fcntl
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

from telethon import functions, types, utils

from video_archive.telegram.archive import BASE, PRIVATE, client_for, confirmed_target, load_config, private_init, rpc
from video_archive.core import (StopArchive, atomic_json, clean, disk_check, now, record_message,
                  sha256, topic_membership, validate_file)

SOURCE_ID = -1004347610716
PROBE = BASE / 'archive/probe-20261019-175448-662322/message.json'


def error_name(exc):
    # Never log RPC requests, arbitrary exception text or credentials.
    return str(exc) if isinstance(exc, StopArchive) else type(exc).__name__


def topic_record(topic, source_id, reason=''):
    deleted = isinstance(topic, types.ForumTopicDeleted)
    title = getattr(topic, 'title', None)
    missing = deleted or bool(getattr(topic, 'title_missing', False)) or not title
    fields = ('id', 'date', 'peer', 'title', 'top_message', 'closed', 'hidden',
              'pinned', 'short', 'title_missing', 'icon_color', 'icon_emoji_id')
    return {'source_id': source_id, 'topic_id': topic.id, 'topic_title': title,
            'created_at': clean(getattr(topic, 'date', None)),
            'closed': getattr(topic, 'closed', None), 'hidden': getattr(topic, 'hidden', None),
            'collected_at': now(), 'title_missing': missing, 'deleted': deleted,
            'status': 'deleted' if deleted else ('title_missing' if missing else 'available'),
            'reason': reason or ('api_deleted' if deleted else ('api_title_missing' if missing else '')),
            'title_provenance': 'forum_topic_title_at_collection_not_historical_title',
            'raw_topic': {k: clean(getattr(topic, k)) for k in fields if hasattr(topic, k)}}


def associate(record, topics):
    """Join only on (source, topic), preserve caption/title and unknowns."""
    result = dict(record)
    tid, reason = topic_membership(record.get('raw_message'), record['source'])
    topic = topics.get((record['source']['id'], tid))
    result.update(topic_id=tid, topic_assignment_reason=reason,
                  topic_title=topic['topic_title'] if topic and not topic['title_missing'] else None,
                  topic_title_collected_at=topic['collected_at'] if topic else None,
                  topic_title_provenance='forum_topic_title_at_collection_not_historical_title',
                  topic_lookup_status=topic['status'] if topic else 'not_collected',
                  topic_lookup_reason=topic['reason'] if topic else 'topic_unknown_or_not_in_catalog')
    return result


class Catalog:
    """Per-run transactional evidence; never opens archive/state.sqlite3 for writing."""
    def __init__(self, root):
        self.root = Path(root)
        self.db = sqlite3.connect(self.root / 'metadata.sqlite3')
        self.db.executescript('''
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS topics (
                source_id INTEGER, topic_id INTEGER, data TEXT NOT NULL,
                PRIMARY KEY(source_id,topic_id));
            CREATE TABLE IF NOT EXISTS observations (
                number INTEGER PRIMARY KEY, source_id INTEGER, topic_id INTEGER, data TEXT);
            CREATE TABLE IF NOT EXISTS pages (number INTEGER PRIMARY KEY, data TEXT);
            CREATE TABLE IF NOT EXISTS messages (
                source_id INTEGER, message_id INTEGER, data TEXT,
                PRIMARY KEY(source_id,message_id));
        ''')

    def put_topic(self, record):
        args = (record['source_id'], record['topic_id'], json.dumps(record, ensure_ascii=False))
        self.db.execute('INSERT OR REPLACE INTO topics VALUES (?,?,?)', args)
        self.db.execute('INSERT INTO observations(source_id,topic_id,data) VALUES (?,?,?)', args)

    def save(self, audit, topics=(), messages=()):
        with self.db:
            for topic in topics:
                self.put_topic(topic)
            for message in messages:
                self.db.execute('INSERT OR REPLACE INTO messages VALUES (?,?,?)',
                                (message['source']['id'], message['message_id'],
                                 json.dumps(message, ensure_ascii=False)))
            self.db.execute('INSERT INTO pages(data) VALUES (?)', (json.dumps(audit),))

    def topics(self):
        return {(s, t): json.loads(d) for s, t, d in self.db.execute('SELECT * FROM topics')}

    def export(self, report):
        topics = self.topics()
        records = [associate(json.loads(row[0]), topics) for row in self.db.execute(
            'SELECT data FROM messages ORDER BY message_id')]
        videos = [r for r in records if r['video_evidence']]
        atomic_json(self.root / 'topics.json', list(topics.values()))
        for name, rows in [('messages.jsonl', records), ('pages.jsonl', [json.loads(r[0]) for r in
                           self.db.execute('SELECT data FROM pages ORDER BY number')]),
                           ('topic-observations.jsonl', [json.loads(r[0]) for r in
                           self.db.execute('SELECT data FROM observations ORDER BY number')])]:
            tmp = self.root / (name + '.tmp')
            with tmp.open('w', encoding='utf-8') as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + '\n')
            os.replace(tmp, self.root / name)
        dramas = []
        for key, topic in sorted(topics.items()):
            linked = [r for r in videos if (r['source']['id'], r['topic_id']) == key]
            scan = report.get('selected_topic_replies', {})
            dramas.append({**topic, 'observed_video_count': len(linked),
                           'accessible_video_total': None, 'video_count_scope': 'partial_observations',
                           'bounded_message_scan': scan if scan.get('topic_id') == key[1] else None,
                           'all_accessible_videos_archived': False, 'drama_episode_completeness': 'unknown',
                           'video_order': 'message_id_not_playback_order', 'videos': linked})
        atomic_json(self.root / 'dramas.json', dramas)
        fields = ['source_id', 'topic_id', 'topic_title', 'status', 'observed_video_count',
                  'video_count_scope', 'message_ids']
        tmp = self.root / 'dramas.csv.tmp'
        with tmp.open('w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for drama in dramas:
                flat = {**drama, 'message_ids': ','.join(str(r['message_id']) for r in drama['videos'])}
                row = {k: flat.get(k) for k in fields}
                for k, value in row.items():
                    if isinstance(value, str) and value.startswith(('=', '+', '-', '@', '\t', '\r')):
                        row[k] = "'" + value
                writer.writerow(row)
        os.replace(tmp, self.root / 'dramas.csv')
        report.update(catalog_records=len(topics), observed_messages=len(records),
                      observed_videos=len(videos),
                      unassigned_video_ids=[r['message_id'] for r in videos if r['topic_id'] is None],
                      unnamed_video_ids=[r['message_id'] for r in videos if r['topic_title'] is None])
        report['topic_records_with_missing_titles'] = [v['topic_id'] for v in topics.values()
                                                       if v['title_missing']]
        report['selected_topic_membership_anomalies'] = [r['message_id'] for r in records
            if r.get('requested_topic_id') and r['requested_topic_id'] != r['topic_id']]
        if report.get('selected_video_evidence'):
            atomic_json(self.root / 'selected-video-evidence.json', report['selected_video_evidence'])
        atomic_json(self.root / 'report.json', report)


def check_peer(obj, source):
    peer = getattr(obj, 'peer', None) or getattr(obj, 'peer_id', None)
    if peer is not None and utils.get_peer_id(peer) != source['id']:
        raise StopArchive('response_source_mismatch')


async def lookup(client, peer, source, catalog, ids):
    audit = {'kind': 'topics_by_id', 'ids': ids, 'collected_at': now()}
    try:
        result = await rpc(lambda: client(functions.messages.GetForumTopicsByIDRequest(peer, ids)))
        found = {}
        for topic in result.topics:
            check_peer(topic, source)
            if topic.id not in ids:
                raise StopArchive('unexpected_topic_id_in_lookup')
            found[topic.id] = topic_record(topic, source['id'])
        for tid in ids:
            if tid not in found:
                found[tid] = {**topic_record(types.ForumTopicDeleted(tid), source['id']),
                              'deleted': None, 'status': 'unavailable', 'reason': 'not_returned_by_id'}
        catalog.save({**audit, 'status': 'returned'}, found.values())
        return found
    except Exception as exc:
        reason = error_name(exc)
        records = [{**topic_record(types.ForumTopicDeleted(tid), source['id']),
                    'deleted': None, 'status': 'query_failed', 'reason': reason} for tid in ids]
        catalog.save({**audit, 'status': 'query_failed', 'reason': reason}, records)
        raise


async def next_cursor(client, peer, response, source):
    # Honor server order including pins. Do not sort by ID or use max/min ID.
    topic = response.topics[-1]
    if not isinstance(topic, types.ForumTopic):
        raise StopArchive('last_topic_deleted_no_safe_cursor')
    date = topic.date
    if not response.order_by_create_date:
        top = next((m for m in response.messages if m.id == topic.top_message), None)
        if top is None or not getattr(top, 'date', None):
            top = await rpc(lambda: client.get_messages(peer, ids=topic.top_message))
        if top is None or not getattr(top, 'date', None):
            raise StopArchive('last_topic_top_message_unavailable')
        check_peer(top, source)
        date = top.date
    if not date or not topic.top_message or topic.id <= 0:
        raise StopArchive('invalid_topic_cursor')
    return (date, topic.top_message, topic.id)


async def collect_directory(client, peer, source, catalog, pass_number, limit=100, max_pages=1000):
    cursor, used, seen, counts, modes = (None, 0, 0), set(), set(), [], set()
    duplicates = []
    for number in range(1, max_pages + 1):
        disk_check(catalog.root)
        audit = {'kind': 'directory', 'pass': pass_number, 'page': number,
                 'request_cursor': clean(cursor), 'limit': limit, 'collected_at': now()}
        try:
            result = await rpc(lambda: client(functions.messages.GetForumTopicsRequest(
                peer, cursor[0], cursor[1], cursor[2], limit)))
            records = []
            for topic in result.topics:
                check_peer(topic, source)
                records.append(topic_record(topic, source['id']))
            ids = [t.id for t in result.topics]
            repeats = sorted(seen.intersection(ids))
            repeats += sorted({tid for tid in ids if ids.count(tid) > 1})
            duplicates.extend(repeats)
            counts.append(result.count)
            modes.add(bool(result.order_by_create_date))
            seen.update(ids)
            audit.update(topic_ids=ids, duplicate_ids=repeats, api_count=result.count,
                         order_by_create_date=bool(result.order_by_create_date),
                         pinned_ids=[t.id for t in result.topics if getattr(t, 'pinned', False)])
            # Persist the response before attempting any cursor repair network call.
            catalog.save({**audit, 'status': 'response_saved'}, records)
            if len(modes) > 1:
                raise StopArchive('topic_ordering_mode_changed')
            if not ids:
                return {'pass': pass_number, 'reached_empty_page': True, 'pages': number,
                        'topic_ids': sorted(seen), 'api_counts': counts, 'duplicate_ids': duplicates,
                        'ordering_modes': sorted(modes),
                        'general_topic_observed': 1 in seen,
                        'observed_minus_last_api_count': len(seen) - counts[-1],
                        'count_consistent': len(set(counts)) == 1 and counts[-1] == len(seen)}
            next_value = await next_cursor(client, peer, result, source)
            if next_value in used or next_value == cursor:
                raise StopArchive('topic_cursor_cycle')
            used.add(next_value)
            catalog.save({**audit, 'status': 'cursor_advanced', 'next_cursor': clean(next_value)})
            cursor = next_value
            await asyncio.sleep(0.3)
        except Exception as exc:
            catalog.save({**audit, 'status': 'failed', 'reason': error_name(exc)})
            raise
    raise StopArchive('topic_directory_page_budget_exhausted')


async def collect_replies(client, peer, source, catalog, tid, cutoff, limit=100, max_pages=100):
    offset, upper, seen = 0, 0, set()
    for number in range(1, max_pages + 1):
        audit = {'kind': 'replies', 'topic_id': tid, 'page': number, 'offset_id': offset,
                 'upper_id': upper or None, 'cutoff_exclusive': clean(cutoff), 'collected_at': now()}
        try:
            disk_check(catalog.root)
            result = await rpc(lambda: client(functions.messages.GetRepliesRequest(
                peer, tid, offset, cutoff, 0, limit, upper + 1 if upper else 0, 0, 0)))
            messages = result.messages
            ids = [m.id for m in messages]
            audit.update(message_ids=ids, api_count=getattr(result, 'count', None))
            if ids != sorted(set(ids), reverse=True) or seen.intersection(ids) or any(
                    i <= 0 or (offset and i >= offset) or (upper and i > upper) for i in ids):
                raise StopArchive('replies_pagination_boundary_error')
            records = []
            for m in messages:
                check_peer(m, source)
                if getattr(m, 'date', None) and m.date >= cutoff:
                    raise StopArchive('replies_outside_time_boundary')
                r = record_message(m, source)
                r['metadata_origin'] = 'selected_topic_getReplies'
                r['requested_topic_id'] = tid
                # Preserve unknown/conflicting membership rather than forcing API request ID.
                records.append(r)
            catalog.save({**audit, 'status': 'response_saved'}, messages=records)
            if not ids:
                return {'topic_id': tid, 'reached_empty_page': True, 'pages': number,
                        'message_ids': sorted(seen), 'upper_id': upper or None,
                        'cutoff_exclusive': clean(cutoff), 'scope': 'selected_topic_only_not_global_scan'}
            upper = upper or max(ids)
            seen.update(ids)
            offset = min(ids)
            await asyncio.sleep(0.3)
        except Exception as exc:
            catalog.save({**audit, 'status': 'failed', 'reason': error_name(exc)})
            raise
    raise StopArchive('selected_topic_page_budget_exhausted')


def original_state():
    root = BASE / 'archive'
    paths = [root / name for name in ('state.sqlite3', 'progress.json', 'report.json')]
    paths += [PROBE, PROBE.parent / 'report.json']
    return {str(p): sha256(p) for p in paths}


async def collect(client, catalog, report, limit):
    if not await rpc(client.is_user_authorized) or await rpc(client.is_bot):
        raise StopArchive('authorized_user_session_required')
    peer, source = await confirmed_target(client)
    if source['id'] != SOURCE_ID or not source['forum'] or source['type'] != 'supergroup':
        raise StopArchive('expected_whitelisted_forum_required')
    report['source'] = source
    # Metadata queries do not reject noforwards; no media download API is used.
    selected = json.loads(PROBE.read_text())
    if selected['source']['id'] != SOURCE_ID or selected['message_id'] != 6559:
        raise StopArchive('probe_identity_mismatch')
    if topic_membership(selected['raw_message'], source)[0] != 6557:
        raise StopArchive('probe_topic_mismatch')
    first = await lookup(client, peer, source, catalog, [6557])
    report['selected_topic_initial_lookup'] = first[6557]
    catalog.export(report)
    report['directory_passes'] = []
    snapshots = []
    for number in (1, 2):
        report['directory_passes'].append(await collect_directory(
            client, peer, source, catalog, number, limit))
        snapshots.append({k: (v['topic_title'], v['status']) for k, v in catalog.topics().items()
                          if k[1] in report['directory_passes'][-1]['topic_ids']})
        catalog.export(report)
    passes = report['directory_passes']
    stable = passes[0]['topic_ids'] == passes[1]['topic_ids'] and snapshots[0] == snapshots[1]
    report['directory_status'] = ('TWO_PASSES_EMPTY_END_STABLE_OBSERVATION' if stable and
        all(p['count_consistent'] and not p['duplicate_ids'] for p in passes) else
        'EMPTY_END_WITH_COUNT_OR_CONCURRENT_CHANGE_LIMITATIONS')
    report['directory_two_pass_sets_and_titles_equal'] = stable
    if not all(p['count_consistent'] for p in passes):
        report['limits'].append(
            'Directory unique-ID count differs from API count; cause not established. '
            'General topic presence is recorded but not assumed to explain the difference.')
    missing = [t for (s, t), v in catalog.topics().items()
               if v['title_missing'] and not v['deleted']]
    for start in range(0, len(missing), 100):
        await lookup(client, peer, source, catalog, missing[start:start + 100])
    # Only this one topic is scanned, not every topic and not global history.
    cutoff = datetime.fromisoformat(report['started_at']).replace(microsecond=0)
    replies = await collect_replies(client, peer, source, catalog, 6557, cutoff, limit)
    report['selected_topic_replies'] = replies
    fresh = await rpc(lambda: client.get_messages(peer, ids=6559))
    if not fresh or isinstance(fresh, types.MessageEmpty):
        raise StopArchive('selected_video_no_longer_accessible')
    check_peer(fresh, source)
    current = record_message(fresh, source)
    if (current['topic_id'] != 6557 or current['media_id'] != selected['media_id'] or
            current['expected_size'] != selected['expected_size']):
        raise StopArchive('selected_video_identity_or_membership_changed')
    validation = validate_file(selected['download']['local_path'], selected['expected_size'],
                               selected['download']['sha256'])
    current['download'] = {**selected['download'], **validation,
                           'decoded_at': selected['download'].get('decoded_at'),
                           'validation_scope': 'size_sha256_ffprobe_rechecked_prior_full_decode_preserved'}
    current['metadata_origin'] = 'exact_message_refresh_and_existing_probe_file'
    current['requested_topic_id'] = 6557
    current['existing_probe_record_path'] = str(PROBE)
    current['existing_probe_collected_at'] = selected['collected_at']
    catalog.save({'kind': 'selected_message_refresh', 'message_id': 6559, 'collected_at': now()},
                 messages=[current])
    final = await lookup(client, peer, source, catalog, [6557])
    crosscheck = 6559 in replies['message_ids']
    report['selected_video_evidence'] = {
        'source_id': source['id'], 'message_id': 6559, 'topic_id': current['topic_id'],
        'topic_title': final[6557]['topic_title'], 'reply': current['reply'],
        'topic_assignment_reason': current['topic_assignment_reason'],
        'getReplies_contains_message': crosscheck, 'original_probe_matches': True,
        'local_file_revalidated': validation, 'local_path': current['download']['local_path'],
        'topic_title_is_collection_time_name': True}
    catalog.export(report)
    if (not crosscheck or final[6557]['status'] != 'available' or
            report['selected_topic_membership_anomalies']):
        raise StopArchive('selected_topic_mapping_not_fully_verified')
    report['result'] = 'TOPIC_METADATA_VERIFIED' if report['directory_status'] == (
        'TWO_PASSES_EMPTY_END_STABLE_OBSERVATION') else 'METADATA_COLLECTED_WITH_LIMITATIONS'


async def run(catalog, report, limit):
    client = client_for(load_config())
    try:
        await rpc(client.connect)
        await collect(client, catalog, report, limit)
    finally:
        if client.is_connected():
            await asyncio.wait_for(client.disconnect(), timeout=20)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--page-size', type=int, choices=range(1, 101), default=100, metavar='1..100')
    args = parser.parse_args()
    private_init()
    with (PRIVATE / 'process.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('Another archive/login process is running.')
            return 2
        root = BASE / 'archive' / ('topics-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
        root.mkdir(parents=True)
        catalog = Catalog(root)
        report = {'started_at': now(), 'result': 'STARTING', 'mode': 'metadata_only',
                  'media_download_attempts': 0, 'full_backfill_complete': False,
                  'global_history_scanned': False, 'global_video_total': None,
                  'directory_status': 'NOT_COMPLETED',
                  'limits': [
                      'Two directory traversals to actual empty responses, not an atomic server snapshot.',
                      'API counts are observations, not proof of no concurrent changes or historical completeness.',
                      'Pinned topics retain server order; repeated IDs and cursor cycles are audited.',
                      'Only topic 6557 replies and message 6559 queried; other topics have unknown video totals.',
                      'Deleted/inaccessible topics and messages cannot be fully enumerated.',
                      'Current topic titles are not historical names at message publication.',
                      'No episode total, segment interpretation or playback order inferred.',
                      'No global-history crosscheck yet; no media downloads or main archive DB writes.',
                      'Failure preserves evidence; rerun creates a fresh inventory, not a frozen snapshot resume.']}
        code = 0
        before = original_state()
        report['original_state_before'] = before
        try:
            report['free_disk_bytes_before'] = disk_check(root)
            asyncio.run(run(catalog, report, args.page_size))
        except (KeyboardInterrupt, asyncio.CancelledError):
            report.update(result='INTERRUPTED', failure_reason='interrupted')
            code = 130
        except Exception as exc:
            report.update(result='NOT_VERIFIED', failure_reason=error_name(exc))
            code = 2
        finally:
            report['finished_at'] = now()
            report['original_state_after'] = original_state()
            report['original_state_unchanged'] = before == report['original_state_after']
            if not report['original_state_unchanged']:
                report.update(result='NOT_VERIFIED', failure_reason='original_state_changed')
                code = 2
            report['sqlite_integrity_check'] = catalog.db.execute('PRAGMA integrity_check').fetchone()[0]
            catalog.export(report)
            catalog.db.close()
            print(f'report={root / "report.json"} result={report["result"]}', flush=True)
        return code


if __name__ == '__main__':
    raise SystemExit(main())