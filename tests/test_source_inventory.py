"""Source membership comes from fresh Telegram topics, not the archive pool."""
import io
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telethon import functions, types

from video_archive.core import StopArchive, Store, record_message
from video_archive.pipeline.common import PipelineError, snapshot
from video_archive.screening.inventory import collect_source_inventory


DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
FORUM = {'id': -1000000000123, 'bare_id': 123, 'name': 'test', 'username': '',
         'type': 'supergroup', 'forum': True, 'protected': False}


def topic(tid):
    return types.ForumTopic(
        id=tid, date=DATE, peer=types.PeerChannel(123), title=f'Topic {tid}', icon_color=0,
        top_message=tid + 9, read_inbox_max_id=0, read_outbox_max_id=0,
        unread_count=0, unread_mentions_count=0, unread_reactions_count=0,
        unread_poll_votes_count=0, from_id=types.PeerChannel(123),
        notify_settings=types.PeerNotifySettings())


def video(mid, tid):
    doc = types.Document(
        id=9000 + mid, access_hash=123, file_reference=b'test-only', date=DATE,
        mime_type='video/mp4', size=2_500_000, dc_id=2,
        attributes=[types.DocumentAttributeFilename('sample.mp4'),
                    types.DocumentAttributeVideo(duration=10, w=720, h=1280)])
    return types.Message(id=mid, peer_id=types.PeerChannel(123), date=DATE,
                         message='', media=types.MessageMediaDocument(document=doc),
                         reply_to=types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=tid))


def creation(tid):
    return types.MessageService(id=tid, peer_id=types.PeerChannel(123), date=DATE,
                                action=types.MessageActionTopicCreate(f'Topic {tid}', 0))


class SourceClient:
    def __init__(self, *, topics=None, messages=None, fail_topic=None,
                 fail_directory=False, honor_id_bound=True):
        self.topics = topics if topics is not None else [topic(1), topic(20), topic(30)]
        self.messages = messages if messages is not None else {
            20: [creation(20), video(22, 20), video(24, 20)],
            30: [creation(30), video(32, 30)],
        }
        self.fail_topic, self.fail_directory = fail_topic, fail_directory
        self.honor_id_bound = honor_id_bound
        self.requests = []

    async def __call__(self, request):
        self.requests.append(request)
        if isinstance(request, functions.messages.GetForumTopicsRequest):
            if self.fail_directory:
                raise OSError('private provider error must not be persisted')
            ids = [t.id for t in self.topics]
            start = ids.index(request.offset_topic) + 1 if request.offset_topic else 0
            selected = self.topics[start:start + request.limit]
            return SimpleNamespace(topics=selected, messages=[], count=len(ids), order_by_create_date=True)
        if isinstance(request, functions.messages.GetRepliesRequest):
            if request.msg_id == self.fail_topic:
                raise OSError('private provider error must not be persisted')
            selected = sorted(self.messages.get(request.msg_id, []), key=lambda m: m.id, reverse=True)
            if request.offset_id:
                selected = [m for m in selected if m.id < request.offset_id]
            if request.max_id and self.honor_id_bound:
                selected = [m for m in selected if m.id < request.max_id]
            selected = selected[:request.limit]
            return SimpleNamespace(messages=selected, count=len(self.messages.get(request.msg_id, [])))
        raise AssertionError('unexpected request')

    async def download_media(self, *args, **kwargs):
        raise AssertionError('source inventory must not download any media')


class DirectorySequenceClient(SourceClient):
    """Independent directory passes can observe different live source lists."""
    def __init__(self, passes, counts=None):
        super().__init__()
        self.directory_passes, self.directory_counts = passes, counts
        self.pass_number = -1

    async def __call__(self, request):
        if not isinstance(request, functions.messages.GetForumTopicsRequest):
            return await super().__call__(request)
        self.requests.append(request)
        if not request.offset_topic:
            self.pass_number += 1
        ids = self.directory_passes[min(self.pass_number, len(self.directory_passes) - 1)]
        start = ids.index(request.offset_topic) + 1 if request.offset_topic else 0
        count = (self.directory_counts[min(self.pass_number, len(self.directory_counts) - 1)]
                 if self.directory_counts is not None else len(ids))
        return SimpleNamespace(topics=[topic(tid) for tid in ids[start:start + request.limit]],
                               messages=[], count=count, order_by_create_date=True)


class SourceInventoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store(self.root)
        self.store.start(FORUM, '2025-01-02T00:00:00+00:00', 100)
        with self.store.db:
            self.store.set('cutoff_exclusive_utc', '2025-01-02T00:00:00+00:00')
        old = record_message(video(22, 20), FORUM)
        self.store.ingest([old], 101, 22, [22])
        self.store.ingest([], 22, 22, [])
        _, state = self.store.videos()[0]
        state.update(status='success', attempts=2, sha256='existing-verification')
        self.store.update(22, state)

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def proofs(self):
        return {row[0]: json.loads(row[1]) for row in self.store.db.execute(
            'SELECT topic_id,data FROM topic_inventories')}

    async def collect(self, client, **kwargs):
        with ExitStack() as stack:
            stack.enter_context(patch('video_archive.telegram.topics.asyncio.sleep', new=AsyncMock()))
            stack.enter_context(patch('video_archive.telegram.topics.disk_check', return_value=1024**4))
            stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
            return await collect_source_inventory(client, 'peer', FORUM, self.store,
                                                   page_size=2, **kwargs)

    async def test_enumerates_source_topics_and_extra_members_outside_existing_pool(self):
        before_pages = list(self.store.db.execute('SELECT * FROM pages'))
        before_cursor = self.store.get('cursor')
        old_state = self.store.db.execute('SELECT data FROM videos WHERE message_id=22').fetchone()[0]
        old_cover = self.store.db.execute('SELECT data FROM covers WHERE message_id=22').fetchone()[0]
        client = SourceClient()
        report = await self.collect(client)
        self.assertEqual(len(report['directories']), 2)
        self.assertEqual([d['pass'] for d in report['directories']], [1, 2])
        self.assertTrue(self.store.get('source_topic_inventory_complete'))
        self.assertEqual(self.store.get('source_topic_ids'), [20, 30])
        self.assertEqual(report['completed_topic_ids'], [20, 30])
        proofs = self.proofs()
        self.assertEqual([v['message_id'] for v in proofs[20]['video_metadata']], [22, 24])
        self.assertEqual([v['message_id'] for v in proofs[30]['video_metadata']], [32])
        self.assertEqual({r['message_id'] for r, _ in self.store.videos()}, {22, 24, 32})
        self.assertTrue(all(p['complete'] and p['reached_empty_page'] for p in proofs.values()))
        self.assertEqual(proofs[20]['video_metadata'][1]['width'], 720)
        self.assertEqual(proofs[20]['video_metadata'][1]['height'], 1280)
        self.assertEqual(list(self.store.db.execute('SELECT * FROM pages')), before_pages)
        self.assertEqual(self.store.get('cursor'), before_cursor)
        self.assertEqual(self.store.db.execute('SELECT data FROM videos WHERE message_id=22').fetchone()[0], old_state)
        self.assertEqual(self.store.db.execute('SELECT data FROM covers WHERE message_id=22').fetchone()[0], old_cover)
        reply_calls = [r for r in client.requests if isinstance(r, functions.messages.GetRepliesRequest)]
        self.assertEqual({r.msg_id for r in reply_calls}, {20, 30})
        self.assertTrue(all(r.max_id <= 101 for r in reply_calls))
        self.assertTrue(all(r.max_id == 101 for r in reply_calls if not r.offset_id))
        self.assertEqual([t['topic_id'] for t in json.loads((self.root / 'source-topics.json').read_text())], [20, 30])
        # An absent/obsolete pool topics file must not constrain the source-owned
        # selection: topic 30 and message 24 only appeared in this source refresh.
        rows, _ = snapshot(self.root, self.root / 'old-pool-topics-missing.json', require_complete=True)
        self.assertEqual({r['message_id'] for _, _, r, _ in rows}, {22, 24, 32})
        self.assertEqual({d['title'] for d, _, _, _ in rows}, {'Topic 20', 'Topic 30'})
        self.assertTrue(all(v['bitrate_bps'] == 2_000_000 for _, v, _, _ in rows))

    async def test_partial_refresh_cannot_reuse_old_complete_flag_or_proofs(self):
        await self.collect(SourceClient())
        self.assertTrue(self.store.get('source_topic_inventory_complete'))
        with self.assertRaises(OSError):
            await self.collect(SourceClient(fail_topic=30))
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        proofs = self.proofs()
        self.assertTrue(proofs[20]['complete'])
        self.assertFalse(proofs[30]['complete'])
        self.assertNotIn('private provider error', json.dumps(proofs))
        with self.assertRaisesRegex(PipelineError, 'source_topic_inventory_required'):
            snapshot(self.root, self.root / 'old-pool-topics-missing.json', require_complete=True)

    async def test_directory_failure_invalidates_previous_success(self):
        await self.collect(SourceClient())
        with self.assertRaises(OSError):
            await self.collect(SourceClient(fail_directory=True))
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertEqual(self.store.get('source_topic_ids'), [])

    async def test_empty_or_ambiguous_messages_do_not_make_complete_proof(self):
        for bad, reason in (
                (types.MessageEmpty(24, types.PeerChannel(123)), 'message_unavailable'),
                (video(24, 99), 'membership_ambiguous')):
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(StopArchive, reason):
                    await self.collect(SourceClient(topics=[topic(20)], messages={20: [bad]}))
                self.assertFalse(self.store.get('source_topic_inventory_complete'))
                self.assertFalse(self.proofs()[20]['complete'])

    async def test_wrong_source_and_global_id_boundary_are_rejected(self):
        other = video(24, 20)
        other.peer_id = types.PeerChannel(456)
        with self.assertRaisesRegex(StopArchive, 'source_mismatch'):
            await self.collect(SourceClient(topics=[topic(20)], messages={20: [other]}))
        with self.assertRaisesRegex(StopArchive, 'outside_boundary'):
            await self.collect(SourceClient(topics=[topic(20)], messages={20: [video(200, 20)]},
                                            honor_id_bound=False))
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertFalse(self.proofs()[20]['complete'])

    async def test_replies_page_budget_is_not_completion(self):
        with self.assertRaisesRegex(StopArchive, 'page_budget_exhausted'):
            await self.collect(SourceClient(topics=[topic(20)]), max_topic_pages=1)
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertFalse(self.proofs()[20]['complete'])

    async def test_directory_page_budget_is_not_completion(self):
        with self.assertRaisesRegex(StopArchive, 'page_budget_exhausted'):
            await self.collect(SourceClient(), max_directory_pages=1)
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertEqual(self.store.get('source_topic_ids'), [])

    async def test_changed_directory_membership_stops_before_reading_any_topic(self):
        client = DirectorySequenceClient([[1, 20, 30], [1, 20, 40]])
        with self.assertRaisesRegex(StopArchive, 'source_topic_directory_changed'):
            await self.collect(client)
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertEqual(self.store.get('source_topic_ids'), [])
        self.assertFalse(any(isinstance(r, functions.messages.GetRepliesRequest) for r in client.requests))
        self.assertEqual(self.proofs(), {})

    async def test_more_reported_topics_than_observed_is_not_completion(self):
        # A topic moved into an already-read directory page can be missed even
        # without duplicate IDs. The API count must expose this as incomplete.
        client = DirectorySequenceClient([[1, 20]], counts=[3])
        with self.assertRaisesRegex(StopArchive, 'source_topic_directory_count_mismatch'):
            await self.collect(client)
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertFalse(any(isinstance(r, functions.messages.GetRepliesRequest) for r in client.requests))

    async def test_general_is_the_only_allowed_one_topic_count_offset(self):
        client = DirectorySequenceClient([[1, 20, 30]], counts=[2])
        report = await self.collect(client)
        self.assertTrue(self.store.get('source_topic_inventory_complete'))
        self.assertEqual(self.store.get('source_topic_ids'), [20, 30])
        self.assertEqual(len(report['directories']), 2)
        self.assertTrue(all(not d['count_consistent'] for d in report['directories']))
        bad = DirectorySequenceClient([[20, 30]], counts=[1])
        with self.assertRaisesRegex(StopArchive, 'source_topic_directory_count_mismatch'):
            await self.collect(bad)
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertFalse(any(isinstance(r, functions.messages.GetRepliesRequest) for r in bad.requests))

    async def test_directory_count_must_be_stable_across_pages_and_passes(self):
        class ChangingPageCount(DirectorySequenceClient):
            async def __call__(self, request):
                result = await super().__call__(request)
                if isinstance(request, functions.messages.GetForumTopicsRequest) and request.offset_topic:
                    result.count = 4
                return result
        for client in (ChangingPageCount([[1, 20, 30]]),
                       DirectorySequenceClient([[1, 20, 30]], counts=[3, 2])):
            with self.subTest(client=type(client).__name__):
                with self.assertRaisesRegex(StopArchive, 'source_topic_directory_count_changed'):
                    await self.collect(client)
                self.assertFalse(self.store.get('source_topic_inventory_complete'))
                self.assertFalse(any(isinstance(r, functions.messages.GetRepliesRequest) for r in client.requests))


if __name__ == '__main__':
    unittest.main()
