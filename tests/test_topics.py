import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telethon import types

import topic_metadata as tm
from core import StopArchive, now, record_message, topic_membership
from test_archive import DATE, SOURCE, message

FORUM = {**SOURCE, 'forum': True, 'type': 'supergroup'}


def topic(tid, title='同名短剧', top=None, pinned=False, **kwargs):
    return types.ForumTopic(
        id=tid, date=DATE, peer=types.PeerChannel(123), title=title, icon_color=0,
        top_message=top or tid + 100, read_inbox_max_id=0, read_outbox_max_id=0,
        unread_count=999, unread_mentions_count=0, unread_reactions_count=0,
        unread_poll_votes_count=0, from_id=types.PeerChannel(123),
        notify_settings=types.PeerNotifySettings(), pinned=pinned, **kwargs)


def page(topics, count=2, creation=False, messages=None):
    return SimpleNamespace(topics=topics, count=count, order_by_create_date=creation,
                           messages=messages if messages is not None else
                           [message(t.top_message) for t in topics if hasattr(t, 'top_message')])


class TopicTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.catalog = tm.Catalog(self.root)

    def tearDown(self):
        self.catalog.db.close()
        self.tmp.cleanup()

    async def test_short_pages_pins_server_order_to_empty(self):
        # IDs are not sorted chronologically; pinned topic is not the cursor.
        client = AsyncMock(side_effect=[page([topic(20, pinned=True), topic(80)]),
                                        page([topic(3)], count=3), page([], count=3)])
        with patch('topic_metadata.asyncio.sleep', new=AsyncMock()):
            result = await tm.collect_directory(client, None, FORUM, self.catalog, 1)
        self.assertTrue(result['reached_empty_page'])
        self.assertFalse(result['count_consistent'])
        self.assertEqual(result['topic_ids'], [3, 20, 80])
        request = client.call_args_list[1].args[0]
        self.assertEqual((request.offset_id, request.offset_topic), (180, 80))
        self.assertEqual(request.offset_date, DATE)
        self.assertEqual(client.await_count, 3)

    async def test_creation_date_and_activity_date_offsets(self):
        t = topic(8)
        m = message(t.top_message)
        m.date = DATE + timedelta(days=2)
        client = AsyncMock()
        for creation, expected in [(True, DATE), (False, m.date)]:
            value = await tm.next_cursor(client, None, page([t], creation=creation, messages=[m]), FORUM)
            self.assertEqual(value, (expected, t.top_message, t.id))
        client.get_messages.assert_not_awaited()

    async def test_missing_top_message_refresh_or_fail(self):
        client = AsyncMock()
        client.get_messages.return_value = message(108)
        self.assertEqual((await tm.next_cursor(client, None, page([topic(8)], messages=[]), FORUM))[1:],
                         (108, 8))
        client.get_messages.return_value = None
        with self.assertRaisesRegex(StopArchive, 'unavailable'):
            await tm.next_cursor(client, None, page([topic(8)], messages=[]), FORUM)

    async def test_cycle_is_not_completion_and_observations_saved(self):
        client = AsyncMock(return_value=page([topic(8)]))
        with patch('topic_metadata.asyncio.sleep', new=AsyncMock()):
            with self.assertRaisesRegex(StopArchive, 'cycle'):
                await tm.collect_directory(client, None, FORUM, self.catalog, 1)
        self.assertEqual(client.await_count, 2)
        self.assertEqual(len(self.catalog.topics()), 1)
        self.assertIn('topic_cursor_cycle', self.catalog.db.execute(
            'SELECT data FROM pages ORDER BY number DESC LIMIT 1').fetchone()[0])

    async def test_order_change_and_budget_stop(self):
        client = AsyncMock(side_effect=[page([topic(8)]), page([topic(7)], creation=True)])
        with patch('topic_metadata.asyncio.sleep', new=AsyncMock()):
            with self.assertRaisesRegex(StopArchive, 'ordering_mode_changed'):
                await tm.collect_directory(client, None, FORUM, self.catalog, 1)
            client = AsyncMock(return_value=page([topic(6)]))
            with self.assertRaisesRegex(StopArchive, 'budget_exhausted'):
                await tm.collect_directory(client, None, FORUM, self.catalog, 2, max_pages=1)

    async def test_same_page_duplicates_and_empty_page_order_change(self):
        client = AsyncMock(side_effect=[page([topic(8), topic(8)], count=1), page([], count=1)])
        with patch('topic_metadata.asyncio.sleep', new=AsyncMock()):
            result = await tm.collect_directory(client, None, FORUM, self.catalog, 1)
            self.assertEqual(result['duplicate_ids'], [8])
            client = AsyncMock(side_effect=[page([topic(7)]), page([], creation=True)])
            with self.assertRaisesRegex(StopArchive, 'ordering_mode_changed'):
                await tm.collect_directory(client, None, FORUM, self.catalog, 2)

    def test_membership_general_ordinary_reply_creation_and_unknown(self):
        m = message(6559)
        self.assertEqual(topic_membership(m, FORUM)[0], 1)
        m.reply_to = types.MessageReplyHeader(reply_to_msg_id=99, reply_to_top_id=70)
        self.assertEqual(topic_membership(m, FORUM)[0], 1)
        self.assertIsNone(topic_membership(m, SOURCE)[0])
        m.reply_to = types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=6557)
        self.assertEqual(topic_membership(m, FORUM), (6557, 'forum_reply_root_id'))
        m.reply_to.reply_to_top_id = 5000
        self.assertEqual(topic_membership(m, FORUM)[0], 5000)
        m.reply_to = types.MessageReplyHeader(forum_topic=True)
        self.assertIsNone(topic_membership(m, FORUM)[0])
        m.reply_to = types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=8,
                                            reply_to_peer_id=types.PeerChannel(456))
        self.assertIsNone(topic_membership(m, FORUM)[0])
        service = types.MessageService(id=6557, peer_id=types.PeerChannel(123), date=DATE,
                                       action=types.MessageActionTopicCreate('创建时名称', 0))
        self.assertEqual(topic_membership(service, FORUM), (6557, 'topic_creation_service'))
        self.assertIsNone(topic_membership(types.MessageEmpty(1, types.PeerChannel(123)), FORUM)[0])
        self.assertIsNone(topic_membership({'_': 'Message', 'id': 4}, FORUM)[0])

    def test_rename_same_name_composite_identity_and_preserve_caption(self):
        records = [tm.topic_record(topic(7), SOURCE['id']), tm.topic_record(topic(8), SOURCE['id']),
                   tm.topic_record(topic(7), -100999)]
        self.catalog.save({'kind': 'test'}, records)
        self.catalog.save({'kind': 'rename'}, [tm.topic_record(topic(7, '改名'), SOURCE['id'])])
        self.assertEqual(len(self.catalog.topics()), 3)
        self.assertEqual(self.catalog.db.execute('SELECT count(*) FROM observations').fetchone()[0], 4)
        m = message(10, text='标题：原正文标题\n第2集', name='2.1.mp4')
        m.reply_to = types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=7)
        r = record_message(m, FORUM)
        linked = tm.associate(r, self.catalog.topics())
        self.assertEqual(linked['topic_title'], '改名')
        self.assertEqual(linked['title'], '原正文标题')
        self.assertEqual(linked['text'], r['text'])
        self.assertEqual(linked['original_filename'], '2.1.mp4')
        self.assertEqual(tm.associate(r, {})['topic_lookup_status'], 'not_collected')

    async def test_missing_deleted_and_failure_are_distinct(self):
        client = AsyncMock(return_value=page([topic(7, '', title_missing=True),
                                            types.ForumTopicDeleted(8)]))
        result = await tm.lookup(client, None, FORUM, self.catalog, [7, 8, 9])
        self.assertEqual([result[t]['status'] for t in [7, 8, 9]],
                         ['title_missing', 'deleted', 'unavailable'])
        self.assertIsNone(result[9]['deleted'])
        client = AsyncMock(side_effect=RuntimeError('do not log this private payload'))
        with self.assertRaises(RuntimeError):
            await tm.lookup(client, None, FORUM, self.catalog, [10])
        failed = self.catalog.topics()[(SOURCE['id'], 10)]
        self.assertEqual(failed['reason'], 'RuntimeError')

    async def test_response_source_mismatch_rejected(self):
        t = topic(7)
        t.peer = types.PeerChannel(456)
        with self.assertRaisesRegex(StopArchive, 'source_mismatch'):
            await tm.lookup(AsyncMock(return_value=page([t])), None, FORUM, self.catalog, [7])

    async def test_replies_short_page_empty_and_unknown_not_forced(self):
        m = message(10)
        m.reply_to = types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=7)
        empty = types.MessageEmpty(9, types.PeerChannel(123))
        client = AsyncMock(side_effect=[page([], messages=[m]), page([], messages=[empty]),
                                        page([], messages=[])])
        with patch('topic_metadata.asyncio.sleep', new=AsyncMock()):
            result = await tm.collect_replies(client, None, FORUM, self.catalog, 7, DATE + timedelta(days=1))
        self.assertEqual(result['message_ids'], [9, 10])
        self.assertTrue(result['reached_empty_page'])
        request = client.call_args_list[1].args[0]
        self.assertEqual((request.offset_id, request.max_id, request.msg_id), (10, 11, 7))
        rows = [json.loads(r[0]) for r in self.catalog.db.execute('SELECT data FROM messages')]
        self.assertIsNone(next(r for r in rows if r['message_id'] == 9)['topic_id'])

    async def test_replies_duplicate_page_stops(self):
        client = AsyncMock(return_value=page([], messages=[message(10)]))
        with patch('topic_metadata.asyncio.sleep', new=AsyncMock()):
            with self.assertRaisesRegex(StopArchive, 'pagination_boundary'):
                await tm.collect_replies(client, None, FORUM, self.catalog, 7, DATE + timedelta(days=1))

    def test_export_unknown_totals_sanitized_and_no_backfill_files(self):
        t = topic(7, '=危险标题')
        t.access_hash = 'must not persist'
        m = message(10)
        m.reply_to = types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=7)
        self.catalog.save({'kind': 'test'}, [tm.topic_record(t, SOURCE['id'])], [record_message(m, FORUM)])
        report = {}
        self.catalog.export(report)
        drama = json.loads((self.root / 'dramas.json').read_text())[0]
        self.assertIsNone(drama['accessible_video_total'])
        self.assertEqual(drama['observed_video_count'], 1)
        self.assertFalse(drama['all_accessible_videos_archived'])
        self.assertIn("'=危险标题", (self.root / 'dramas.csv').read_text())
        for name in ['messages.jsonl', 'topics.json', 'dramas.json', 'topic-observations.jsonl']:
            text = (self.root / name).read_text()
            self.assertNotIn('access_hash', text)
            self.assertNotIn('file_reference', text)
        self.assertFalse((self.root / 'state.sqlite3').exists())

    async def test_unauthorized_does_not_resolve_or_download(self):
        client = AsyncMock()
        client.is_user_authorized.return_value = False
        with patch('topic_metadata.confirmed_target', new=AsyncMock()) as resolve:
            with self.assertRaisesRegex(StopArchive, 'authorized_user'):
                await tm.collect(client, self.catalog, {}, 100)
        resolve.assert_not_awaited()
        client.download_media.assert_not_awaited()

    async def test_protected_metadata_end_to_end_without_download_or_original_mutation(self):
        source = {**FORUM, 'id': tm.SOURCE_ID, 'bare_id': 4347610716, 'protected': True}
        t = topic(6557, '真实接口测试名称', top=6559)
        t.peer = types.PeerChannel(source['bare_id'])
        m = message(6559)
        m.peer_id = t.peer
        m.reply_to = types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=6557)
        original = record_message(m, source)
        original['download'] = {'local_path': str(self.root / 'existing.mp4'),
                                'sha256': 'test-only', 'decoded_at': DATE.isoformat(), 'status': 'success'}
        probe = self.root / 'probe.json'
        probe.write_text(json.dumps(original))
        before = probe.read_bytes()
        replies = page([], messages=[m])
        client = AsyncMock(side_effect=[page([t]), page([t], count=1, messages=[m]), page([], count=1),
            page([t], count=1, messages=[m]), page([], count=1), replies, page([], messages=[]), page([t])])
        client.is_user_authorized.return_value = True
        client.is_bot.return_value = False
        client.get_messages.return_value = m
        report = {'started_at': now(), 'limits': []}
        with patch('topic_metadata.PROBE', probe), \
                patch('topic_metadata.confirmed_target', new=AsyncMock(return_value=(None, source))), \
                patch('topic_metadata.asyncio.sleep', new=AsyncMock()), \
                patch('topic_metadata.validate_file', return_value={'verified_at': now(), 'decoded_at': None}):
            await tm.collect(client, self.catalog, report, 100)
        self.assertEqual(report['result'], 'TOPIC_METADATA_VERIFIED')
        self.assertTrue(report['source']['protected'])
        self.assertTrue(report['selected_video_evidence']['getReplies_contains_message'])
        self.assertEqual(report['selected_topic_membership_anomalies'], [])
        client.download_media.assert_not_awaited()
        self.assertEqual(before, probe.read_bytes())
        self.catalog.export(report)
        self.assertTrue((self.root / 'selected-video-evidence.json').exists())