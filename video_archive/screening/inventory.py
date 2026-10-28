"""Fresh, source-owned forum-topic membership for whole-drama planning.

The archive/pool is a cache, never the list of episodes to enumerate. A run first
walks the Telegram topic directory, then walks every non-General topic to an
empty replies page within the existing frozen history boundary. This collects
metadata only and does not transfer video or cover bytes.
"""
import json
import tempfile
from datetime import datetime
from pathlib import Path

from telethon import functions, types, utils

from video_archive.core import StopArchive, atomic_json, now
from video_archive.telegram.topics import Catalog, collect_directory, collect_replies, error_name


class _BoundedRepliesClient:
    """Apply the archive's global ID bound even on a topic's first page."""
    def __init__(self, client, upper_id):
        self.client, self.upper_id = client, upper_id

    async def __call__(self, request):
        if not isinstance(request, functions.messages.GetRepliesRequest):
            raise StopArchive('source_inventory_unexpected_request')
        maximum = self.upper_id + 1
        if request.max_id:
            maximum = min(maximum, request.max_id)
        bounded = functions.messages.GetRepliesRequest(
            request.peer, request.msg_id, request.offset_id, request.offset_date,
            request.add_offset, request.limit, maximum, request.min_id, request.hash)
        return await self.client(bounded)


def _peer_id(raw):
    peer = raw.get('peer_id') or raw.get('peer')
    if not isinstance(peer, dict):
        return None
    for kind, field, constructor in (
            ('PeerChannel', 'channel_id', types.PeerChannel),
            ('PeerChat', 'chat_id', types.PeerChat),
            ('PeerUser', 'user_id', types.PeerUser)):
        if peer.get('_') == kind and type(peer.get(field)) is int:
            return utils.get_peer_id(constructor(peer[field]))
    return None


def _validate_records(records, reply_result, source_id, topic_id, upper_id, cutoff):
    if not reply_result.get('reached_empty_page'):
        raise StopArchive('source_topic_replies_incomplete')
    expected = reply_result.get('message_ids')
    if not isinstance(expected, list) or len(expected) != len(set(expected)):
        raise StopArchive('source_topic_message_set_invalid')
    if {r.get('message_id') for r in records} != set(expected):
        raise StopArchive('source_topic_message_set_mismatch')
    videos = []
    for record in records:
        raw = record.get('raw_message') or {}
        if raw.get('_') == 'MessageEmpty':
            raise StopArchive('source_topic_message_unavailable')
        if (record.get('source') or {}).get('id') != source_id or _peer_id(raw) != source_id:
            raise StopArchive('source_topic_source_mismatch')
        if (record.get('requested_topic_id') != topic_id
                or record.get('topic_id') != topic_id):
            raise StopArchive('source_topic_membership_ambiguous')
        mid = record.get('message_id')
        if type(mid) is not int or not 0 < mid <= upper_id:
            raise StopArchive('source_topic_id_outside_boundary')
        try:
            published = datetime.fromisoformat(record['published_at'])
            if published.tzinfo is None or published >= cutoff:
                raise ValueError()
        except (KeyError, TypeError, ValueError):
            raise StopArchive('source_topic_time_outside_boundary') from None
        if 'PaidMedia' in (raw.get('media') or {}).get('_', ''):
            raise StopArchive('source_topic_paid_media_unavailable')
        if record.get('video_evidence'):
            media_id = record.get('media_id')
            if type(media_id) is not int or media_id <= 0:
                raise StopArchive('source_topic_video_identity_missing')
            videos.append({key: record.get(key) for key in (
                'message_id', 'media_id', 'expected_size', 'duration_seconds', 'width', 'height')})
    return sorted(videos, key=lambda r: r['message_id'])


def _validate_directory(directory):
    """Validate one independent pass, allowing only the known General offset."""
    if not directory.get('reached_empty_page') or directory.get('duplicate_ids'):
        raise StopArchive('source_topic_directory_incomplete')
    ids, counts = directory.get('topic_ids'), directory.get('api_counts')
    if (not isinstance(ids, list) or any(type(tid) is not int or tid <= 0 for tid in ids)
            or len(ids) != len(set(ids))):
        raise StopArchive('source_topic_directory_set_invalid')
    if (not isinstance(counts, list) or not counts
            or any(type(count) is not int or count < 0 for count in counts)
            or len(set(counts)) != 1):
        raise StopArchive('source_topic_directory_count_changed')
    seen = set(ids)
    allowed_counts = {len(seen)}
    if 1 in seen:
        allowed_counts.add(len(seen) - 1)
    if counts[0] not in allowed_counts:
        raise StopArchive('source_topic_directory_count_mismatch')
    return seen, counts[0]


async def collect_source_inventory(client, peer, source, store, *, page_size=100,
                                   max_directory_pages=10000, max_topic_pages=10000):
    """Refresh every source topic, independently of existing videos/dramas.

    The global completion flag is invalidated before work starts. Old matching
    proofs can never substitute for an unsuccessful refresh. Two independent
    directory passes must agree before any topic's messages are fetched.
    Per-topic success
    means the accessible API traversal reached an empty page, not an atomic
    server snapshot or proof of deleted/invisible historical media.
    """
    with store.db:
        store.set('source_topic_inventory_complete', False)
        store.set('source_topic_ids', [])
    frozen_source = store.get('source') or {}
    upper_id = store.get('upper_id')
    cutoff_text = store.get('cutoff_exclusive_utc')
    if (not source.get('forum') or source.get('id') != frozen_source.get('id')
            or not store.get('scan_complete') or type(upper_id) is not int or upper_id < 0):
        raise StopArchive('source_inventory_boundary_invalid')
    try:
        cutoff = datetime.fromisoformat(cutoff_text)
        if cutoff.tzinfo is None:
            raise ValueError()
    except (TypeError, ValueError):
        raise StopArchive('source_inventory_boundary_invalid') from None
    if (type(page_size) is not int or not 1 <= page_size <= 100
            or type(max_directory_pages) is not int or max_directory_pages <= 0
            or type(max_topic_pages) is not int or max_topic_pages <= 0):
        raise StopArchive('source_inventory_limits_invalid')
    boundary = {'source_id': source['id'], 'upper_id': upper_id,
                'cutoff_exclusive_utc': cutoff_text}
    root = Path(tempfile.mkdtemp(prefix='source-inventory-', dir=store.root))
    catalog = Catalog(root)
    report = {'at': now(), 'boundary': boundary, 'status': 'in_progress',
              'scope': 'accessible_source_topics_within_frozen_boundary',
              'not_atomic_server_snapshot': True, 'directories': [], 'topics': []}
    with store.db:
        store.set('source_topic_inventory_boundary', boundary)
        store.set('source_topic_inventory_audit', root.name)
    completed = []
    current = None
    exported = False
    try:
        directory_ids, directory_count = None, None
        for pass_number in (1, 2):
            directory = await collect_directory(client, peer, source, catalog, pass_number,
                                                limit=page_size, max_pages=max_directory_pages)
            report['directories'].append(directory)
            report['directory'] = directory
            ids, count = _validate_directory(directory)
            if directory_ids is not None and ids != directory_ids:
                raise StopArchive('source_topic_directory_changed')
            if directory_count is not None and count != directory_count:
                raise StopArchive('source_topic_directory_count_changed')
            directory_ids, directory_count = ids, count
        topics = catalog.topics()
        topic_ids = sorted(tid for sid, tid in topics if sid == source['id'] and tid > 1)
        listed_ids = sorted(tid for tid in directory_ids if tid > 1)
        if topic_ids != listed_ids:
            raise StopArchive('source_topic_directory_set_mismatch')
        with store.db:
            store.set('source_topic_ids', topic_ids)
        atomic_json(store.root / 'source-topics.json',
                    [topics[(source['id'], tid)] for tid in topic_ids])
        bounded = _BoundedRepliesClient(client, upper_id)
        observed_membership = {}
        for topic_id in topic_ids:
            topic = topics[(source['id'], topic_id)]
            current = {**boundary, 'topic_id': topic_id,
                       'topic_title': topic.get('topic_title'),
                       'complete': False, 'reached_empty_page': False,
                       'video_metadata': [], 'collected_at': now(),
                       'audit_directory': root.name}
            store.save_topic_inventory(current)
            if topic.get('deleted') or topic.get('title_missing') or not topic.get('topic_title'):
                raise StopArchive('source_topic_metadata_unavailable')
            replies = await collect_replies(bounded, peer, source, catalog, topic_id, cutoff,
                                             limit=page_size, max_pages=max_topic_pages)
            records = [json.loads(row[0]) for row in catalog.db.execute(
                'SELECT data FROM messages ORDER BY message_id')]
            records = [r for r in records if r.get('requested_topic_id') == topic_id]
            video_metadata = _validate_records(records, replies, source['id'], topic_id, upper_id, cutoff)
            for record in records:
                previous_topic = observed_membership.setdefault(record['message_id'], topic_id)
                if previous_topic != topic_id:
                    raise StopArchive('source_message_topic_conflict')
            store.ingest_topic_records(records)
            current.update(complete=True, reached_empty_page=True, video_metadata=video_metadata,
                           message_count=len(records), replies=replies)
            store.save_topic_inventory(current)
            completed.append(topic_id)
            report['topics'].append({'topic_id': topic_id, 'video_count': len(video_metadata),
                                     'message_count': len(records), 'complete': True})
            print(f'source topic={topic_id} videos={len(video_metadata)} '
                  f'completed={len(completed)}/{len(topic_ids)}', flush=True)
            current = None
        report.update(status='source_topic_inventory_complete', completed_topic_ids=completed)
        catalog.export(report)
        exported = True
        with store.db:
            store.set('source_topic_inventory_complete', True)
        return report
    except BaseException as exc:
        reason = error_name(exc)
        report.update(status='incomplete', reason=reason, completed_topic_ids=completed)
        with store.db:
            store.set('source_topic_inventory_complete', False)
        if current is not None:
            current.update(complete=False, reason=reason)
            store.save_topic_inventory(current)
        raise
    finally:
        try:
            if not exported:
                catalog.export(report)
        finally:
            catalog.db.close()
