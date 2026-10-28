import asyncio
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import video_archive.telegram.archive as archive
from video_archive.core import (Store, StopArchive, classify, clean, cover_filename, disk_check,
                  filename, pick_thumb, record_message, validate_cover, validate_file)
from telethon import errors, types


COVER = b'\xff\xd8\xff' + b'jpeg-cover-bytes'
THUMBS = {'m': 7, 'x': len(COVER)}
SOURCE = {'id': -1000000000123, 'bare_id': 123, 'name': 'test', 'username': '',
          'type': 'channel', 'forum': False, 'protected': False}
DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)


def message(mid, size=10, text='', mime='video/mp4', name='same.mp4', video=True, thumbs=True):
    attrs = [types.DocumentAttributeFilename(name)]
    if video:
        attrs.append(types.DocumentAttributeVideo(duration=1, w=64, h=64))
    sizes = [types.PhotoStrippedSize(type='i', bytes=b'inline-blur'),
             types.PhotoSize(type='m', w=32, h=32, size=THUMBS['m']),
             types.PhotoSize(type='x', w=320, h=320, size=THUMBS['x'])] if thumbs else []
    doc = types.Document(id=9000 + mid, access_hash=987654, file_reference=b'secret',
                         date=DATE, mime_type=mime, size=size, dc_id=2, attributes=attrs,
                         thumbs=sizes)
    return types.Message(id=mid, peer_id=types.PeerChannel(123), date=DATE,
                         message=text, media=types.MessageMediaDocument(document=doc))


class FakeClient:
    def __init__(self, messages, content=b'', fail=0, interrupt=False, cover=COVER, cover_fail=0):
        self.messages = messages
        self.content = content
        self.cover, self.cover_fail = cover, cover_fail
        self.thumb_downloads, self.thumbs_requested = 0, []
        self.downloads = 0
        self.fail = fail
        self.interrupt = interrupt
        self.requests = []

    async def __call__(self, request):
        self.requests.append(request)
        candidates = [m for m in self.messages if m.id < request.offset_id and
                      m.id < request.max_id and m.date < request.offset_date]
        # Intentionally short nonempty pages: don't assume len(page)==limit.
        return types.messages.Messages(messages=candidates[:2], topics=[], chats=[], users=[])

    async def get_entity(self, peer):
        return types.Channel(id=123, title='test', photo=types.ChatPhotoEmpty(), date=DATE,
                             broadcast=True, access_hash=987654)

    async def get_messages(self, peer, ids):
        return next((m for m in self.messages if m.id == ids), None)

    async def download_media(self, msg, file, progress_callback=None, thumb=None):
        if thumb is not None:
            # Covers come from the message's own thumbnail, never from video bytes.
            self.thumb_downloads += 1
            self.thumbs_requested.append(thumb)
            if self.cover_fail:
                self.cover_fail -= 1
                raise errors.FileReferenceExpiredError(request=None)
            file.write(self.cover)
            return
        self.downloads += 1
        file.write(self.content[:10])
        if self.interrupt:
            self.interrupt = False
            raise asyncio.CancelledError()
        if self.fail:
            self.fail -= 1
            raise errors.FileReferenceExpiredError(request=None)
        file.seek(0)
        file.write(self.content)
        await progress_callback(len(self.content), len(self.content))


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.media_dir = tempfile.TemporaryDirectory()
        path = Path(cls.media_dir.name) / 'fixture.mp4'
        subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                        'color=c=blue:s=64x64:d=0.3', '-c:v', 'mpeg4', '-y', str(path)], check=True)
        cls.content = path.read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls.media_dir.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(self.tmp.name)
        self.store.start(SOURCE, '2025-01-02T00:00:00+00:00', 10)
        with self.store.db:
            self.store.set('cutoff_exclusive_utc', '2025-01-02T00:00:00+00:00')

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def add(self, messages):
        ids = [m.id for m in messages]
        self.store.ingest([record_message(m, SOURCE) for m in messages], 11, min(ids), ids)

    async def test_cover_download_picks_largest_thumbnail_and_stays_off_ffmpeg(self):
        msgs = [message(10, len(self.content))]
        self.add(msgs)
        record, _ = self.store.videos()[0]
        self.assertEqual([t['type'] for t in record['thumbs']], ['m', 'x'])
        client = FakeClient(msgs, self.content)
        cover = self.store.cover(10)
        with patch('video_archive.core.media_tools', side_effect=AssertionError('covers must not touch ffmpeg')):
            result = await archive.download_cover(client, None, self.store, record, cover)
        self.assertEqual(result, 'downloaded_success')
        # Index 2 in doc.thumbs: the stripped inline blur at index 0 is not fetchable.
        self.assertEqual(client.thumbs_requested, [2])
        stored = self.store.cover(10)
        self.assertEqual(stored['status'], 'success')
        self.assertEqual(stored['local_path'], 'covers/' + filename(record) + '.jpg')
        path = Path(self.tmp.name) / stored['local_path']
        self.assertEqual(path.read_bytes(), COVER)
        self.assertEqual(stored['sha256'], hashlib.sha256(COVER).hexdigest())
        self.assertEqual(stored['thumb']['type'], 'x')
        self.assertFalse(list((Path(self.tmp.name) / 'covers').glob('*.part')))
        # Second pass verifies the stored file instead of fetching it again.
        self.assertEqual(await archive.download_cover(client, None, self.store, record, stored),
                         'verified_skipped')
        self.assertEqual(client.thumb_downloads, 1)

    async def test_cover_absent_thumbnail_is_terminal_and_recorded(self):
        msgs = [message(10, len(self.content), thumbs=False)]
        self.add(msgs)
        record, _ = self.store.videos()[0]
        self.assertEqual(record['thumbs'], [])
        client = FakeClient(msgs, self.content)
        result = await archive.download_cover(client, None, self.store, record, self.store.cover(10))
        self.assertEqual(result, 'no_thumbnail')
        stored = self.store.cover(10)
        self.assertEqual((stored['status'], stored['failure_reason']),
                         ('unavailable', 'no_thumbnail_available'))
        self.assertEqual(stored['local_path'], '')
        self.assertEqual(client.thumb_downloads, 0)

    async def test_cover_failure_leaves_the_video_untouched_and_vice_versa(self):
        msgs = [message(10, len(self.content))]
        self.add(msgs)
        record, state = self.store.videos()[0]
        # Exhaust every retry attempt: a transient failure on only some of them
        # is now recovered automatically, same as the video's own download.
        client = FakeClient(msgs, self.content, cover_fail=archive.RETRIES)
        result = await archive.download_cover(client, None, self.store, record, self.store.cover(10))
        self.assertEqual(result, 'failed')
        cover = self.store.cover(10)
        self.assertEqual((cover['status'], cover['attempts']), ('failed', archive.RETRIES))
        self.assertEqual(cover['failure_reason'], 'FileReferenceExpiredError')
        self.assertFalse(list((Path(self.tmp.name) / 'covers').glob('*')))
        # The video is still pending, with no attempt and no failure of its own.
        _, state = self.store.videos()[0]
        self.assertEqual((state['status'], state['attempts'], state['failure_reason']),
                         ('pending', 0, ''))
        # And a video download leaves the cover's own failed state alone.
        outcome = await archive.download_one(client, None, SOURCE, self.store, record, state)
        self.assertEqual(outcome, 'downloaded_success')
        self.assertEqual(self.store.cover(10)['status'], 'failed')

    async def test_cover_rejects_a_non_jpeg_payload(self):
        msgs = [message(10, len(self.content))]
        self.add(msgs)
        record, _ = self.store.videos()[0]
        # Same length as a real cover, so only the JPEG check can reject it.
        client = FakeClient(msgs, self.content, cover=b'<html>not an image!')
        result = await archive.download_cover(client, None, self.store, record, self.store.cover(10))
        self.assertEqual(result, 'failed')
        self.assertIn('cover_not_jpeg', self.store.cover(10)['failure_reason'])
        self.assertFalse(list((Path(self.tmp.name) / 'covers').glob('*')))

    async def test_pagination_short_pages_gaps_fixed_boundary_and_resume(self):
        msgs = [message(i) for i in [12, 10, 8, 6, 2, 1]]
        client = FakeClient(msgs)
        self.add(msgs[1:3])
        await archive.scan(client, None, self.store, 100)
        self.assertEqual(self.store.pagination_audit()['unique_ids'], 5)
        self.assertTrue(self.store.pagination_audit()['ok'])
        self.assertTrue(self.store.get('scan_complete'))
        self.assertEqual([r.offset_id for r in client.requests], [8, 2, 1])
        count = len(client.requests)
        await archive.scan(client, None, self.store, 100)
        self.assertEqual(count, len(client.requests))

    async def test_download_empty_fields_names_repeated_run_and_report(self):
        msgs = [message(10, len(self.content)), message(9, len(self.content))]
        self.add(msgs)
        self.store.ingest([], 9, 9, [])
        client = FakeClient(msgs, self.content)
        for r, s in self.store.videos():
            self.assertEqual((r['title'], r['description'], r['tags']), ('', '', []))
            self.assertEqual(await archive.download_one(client, None, SOURCE, self.store, r, s, True), 'downloaded_success')
        paths = [s['local_path'] for _, s in self.store.videos()]
        self.assertNotEqual(paths[0], paths[1])
        for r, s in self.store.videos():
            self.assertEqual(await archive.download_one(client, None, SOURCE, self.store, r, s), 'verified_skipped')
        self.assertEqual(client.downloads, 2)
        with self.store.db:
            self.store.set('sample_ids', [10, 9])
        self.assertTrue(archive.verify(self.store))
        self.store.export()
        report = self.store.report()
        self.assertEqual(report['result'], 'COMPLETE_VERIFIED')
        raw = (self.store.root / 'messages.jsonl').read_text()
        self.assertNotIn('access_hash', raw)
        self.assertNotIn('file_reference', raw)
        for line in raw.splitlines():
            item = json.loads(line)
            self.assertEqual(item['source']['id'], SOURCE['id'])
            self.assertEqual(item['download']['actual_size'], len(self.content))
        with (self.store.root / 'index.csv').open(encoding='utf-8-sig') as f:
            self.assertEqual(next(csv.DictReader(f))['tags'], '')

    async def test_interruption_restart_and_failure_retry(self):
        msg = message(10, len(self.content))
        self.add([msg])
        client = FakeClient([msg], self.content, interrupt=True)
        r, s = self.store.videos()[0]
        with self.assertRaises(asyncio.CancelledError):
            await archive.download_one(client, None, SOURCE, self.store, r, s)
        self.assertEqual(self.store.videos()[0][1]['status'], 'pending')
        self.assertEqual(len(list((self.store.root / 'videos').glob('*.part'))), 1)
        # Reopen durable database as a new process would.
        self.store.db.close()
        self.store = Store(self.tmp.name)
        r, s = self.store.videos()[0]
        client.content = b'invalid'
        self.assertEqual(await archive.download_one(client, None, SOURCE, self.store, r, s), 'failed')
        client.content = self.content
        operation = {}
        await archive.download_set(client, None, SOURCE, self.store, operation, only_failed=True)
        self.assertEqual(operation, {'downloaded_success': 1})
        self.assertEqual(self.store.videos()[0][1]['status'], 'success')
        self.assertFalse(list((self.store.root / 'videos').glob('*.part')))

    async def test_expired_file_reference_refresh(self):
        msg = message(10, len(self.content))
        self.add([msg])
        client = FakeClient([msg], self.content, fail=1)
        r, s = self.store.videos()[0]
        self.assertEqual(await archive.download_one(client, None, SOURCE, self.store, r, s), 'downloaded_success')
        self.assertEqual(client.downloads, 2)

    async def test_changed_media_never_downloads(self):
        msg = message(10, len(self.content))
        self.add([msg])
        r, s = self.store.videos()[0]
        client = FakeClient([msg], self.content)
        msg.media.document.id += 1
        self.assertEqual(await archive.download_one(client, None, SOURCE, self.store, r, s), 'failed')
        self.assertEqual(client.downloads, 0)

    async def test_hash_tampering_cannot_remain_success(self):
        msg = message(10, len(self.content))
        self.add([msg])
        r, s = self.store.videos()[0]
        client = FakeClient([msg], self.content)
        await archive.download_one(client, None, SOURCE, self.store, r, s)
        r, s = self.store.videos()[0]
        path = self.store.root / s['local_path']
        content = bytearray(path.read_bytes())
        content[-1] ^= 1
        path.write_bytes(content)
        self.assertFalse(archive.verify(self.store))
        self.assertEqual(self.store.videos()[0][1]['status'], 'failed')
        self.assertEqual(self.store.report()['result'], 'INCOMPLETE')

    async def test_flood_wait_budget_and_network_retry_budget(self):
        calls = 0
        async def flood():
            raise errors.FloodWaitError(request=None, capture=120)
        with self.assertRaises(StopArchive):
            await archive.rpc(flood)
        async def broken():
            nonlocal calls
            calls += 1
            raise ConnectionError()
        with patch('video_archive.telegram.archive.asyncio.sleep', return_value=None):
            with self.assertRaises(ConnectionError):
                await archive.rpc(broken)
        self.assertEqual(calls, 3)

    def test_classification_relationships_sanitization(self):
        for kwargs, evidence in [({'name': 'unknown.bin', 'mime': 'application/octet-stream'}, 'DocumentAttributeVideo'),
                                 ({'name': 'unknown.bin', 'video': False}, 'video_mime'),
                                 ({'video': False, 'mime': 'application/octet-stream'}, 'video_extension_candidate')]:
            self.assertIn(evidence, classify(message(10, **kwargs))[0])
        m = message(10, name='../../危险/name.mp4', text='标题：明确标题\n简介：说明\n集数：2\n#标签')
        m.grouped_id = 567
        m.reply_to = types.MessageReplyHeader(reply_to_msg_id=4, reply_to_top_id=2, forum_topic=True)
        r = record_message(m, SOURCE)
        self.assertEqual(r['topic_id'], 2)
        self.assertEqual(r['album_id'], 567)
        self.assertEqual(r['title'], '明确标题')
        self.assertEqual(r['tags'], ['标签'])
        self.assertNotIn('/', filename(r))
        self.assertEqual(clean({'nested': {'access_hash': 2, 'file_reference': b'a', 'ok': 1}}), {'nested': {'ok': 1}})
        text_msg = types.Message(id=9, peer_id=types.PeerChannel(123), date=DATE, message='相册说明')
        self.add([m, text_msg])
        report = self.store.report()
        self.assertEqual(report['excluded_nonvideo_messages'], 1)
        self.assertEqual(report['scanned_messages'], 2)
        self.assertTrue(report['reconciliation_ok'])

    def test_disk_deficit(self):
        with patch('video_archive.core.shutil.disk_usage', return_value=shutil._ntuple_diskusage(100, 99, 1)):
            with self.assertRaisesRegex(StopArchive, 'deficit='):
                disk_check(self.store.root, 100)

    async def test_protected_source_reaches_scan(self):
        client = AsyncMock()
        client.is_connected = Mock(return_value=True)
        client.is_user_authorized.return_value = True
        client.is_bot.return_value = False
        protected = {**SOURCE, 'protected': True}
        args = archive.parser().parse_args(['sample'])
        with patch('video_archive.telegram.archive.load_config', return_value={}), \
                patch('video_archive.telegram.archive.client_for', return_value=client), \
                patch('video_archive.telegram.archive.confirmed_target', new=AsyncMock(return_value=(None, protected))), \
                patch('video_archive.telegram.archive.ensure_task', new=AsyncMock()) as ensure, \
                patch('video_archive.telegram.archive.scan', new=AsyncMock(side_effect=StopArchive('scan_reached'))) as scan:
            with self.assertRaisesRegex(StopArchive, 'scan_reached'):
                await archive.execute(args, self.store)
            ensure.assert_awaited_once()
            scan.assert_awaited_once()
        self.assertTrue(self.store.get('preflight')['download_allowed'])

    async def test_protection_flags_and_legacy_block_do_not_stop_download(self):
        msg = message(10, len(self.content))
        msg.noforwards = True
        protected = {**SOURCE, 'protected': True}
        self.store.ingest([record_message(msg, protected)], 11, 10, [10])
        r, s = self.store.videos()[0]
        self.assertTrue(r['protected'])
        self.assertEqual(s['status'], 'pending')
        s.update(status='blocked', failure_reason='content_protected')
        self.store.update(10, s)
        self.store.prepare_downloads()
        self.assertEqual(archive.choose_sample(self.store, 1), [10])
        self.assertEqual(self.store.remaining_bytes(), len(self.content))
        client = FakeClient([msg], self.content)
        entity = await client.get_entity(None)
        entity.noforwards = True
        client.get_entity = AsyncMock(return_value=entity)
        r, s = self.store.videos()[0]
        self.assertEqual(await archive.download_one(client, None, protected, self.store, r, s), 'downloaded_success')
        self.assertEqual(client.downloads, 1)

    async def test_general_excluded_with_consistent_report(self):
        source = {**SOURCE, 'forum': True}
        msg = message(10, len(self.content))
        record = record_message(msg, source)
        self.assertEqual(record['topic_id'], 1)
        self.store.ingest([record], 11, 10, [10])
        self.store.ingest([], 10, 10, [])
        self.assertEqual(self.store.videos()[0][1]['status'], 'skipped')
        self.assertEqual(archive.choose_sample(self.store, 3), [])
        self.assertEqual(self.store.remaining_bytes(), 0)
        client = FakeClient([msg], self.content)
        await archive.download_set(client, None, source, self.store, {})
        self.assertEqual(client.downloads, 0)
        archive.verify(self.store)
        report = self.store.report()
        self.assertTrue(report['reconciliation_ok'])
        self.assertEqual(report['result'], 'COMPLETE_VERIFIED')
        self.assertEqual(report['video_states_mutually_exclusive']['skipped'], 1)

    async def test_expiring_media_still_blocked(self):
        msg = message(10, len(self.content))
        msg.media.ttl_seconds = 10
        self.add([msg])
        client = FakeClient([msg], self.content)
        r, s = self.store.videos()[0]
        self.assertEqual(await archive.download_one(client, None, SOURCE, self.store, r, s), 'blocked')
        self.store.prepare_downloads()
        self.assertEqual(self.store.videos()[0][1]['failure_reason'], 'expiring_media')
        self.assertEqual(client.downloads, 0)

    def test_legacy_general_invalidates_sample_without_deleting_file(self):
        record = record_message(message(10), {**SOURCE, 'forum': True})
        self.store.ingest([record], 11, 10, [10])
        r, s = self.store.videos()[0]
        path = self.store.root / 'videos' / filename(r)
        path.write_bytes(self.content)
        s.update(status='success', local_path=str(path.relative_to(self.store.root)))
        self.store.update(10, s)
        with self.store.db:
            self.store.set('sample_ids', [10])
            self.store.set('sample_passed', True)
        self.store.prepare_downloads()
        self.assertEqual(self.store.videos()[0][1]['status'], 'skipped')
        self.assertEqual(self.store.get('sample_ids'), [])
        self.assertFalse(self.store.get('sample_passed'))
        self.assertTrue(path.exists())

    async def test_two_concurrent_downloads_finish_and_verify(self):
        msgs = [message(i, len(self.content)) for i in (10, 9, 8)]
        self.add(msgs)
        client = FakeClient(msgs, self.content)
        download = client.download_media
        active = 0
        peak = 0
        started = asyncio.Event()

        async def overlapping_download(*args, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 2:
                started.set()
            try:
                await started.wait()
                await download(*args, **kwargs)
            finally:
                active -= 1

        client.download_media = overlapping_download
        operation = {}
        await asyncio.wait_for(archive.download_set(client, None, SOURCE, self.store, operation), 5)
        self.assertEqual(peak, 2)
        self.assertEqual(active, 0)
        self.assertEqual(operation, {'downloaded_success': 3})
        self.assertTrue(archive.verify(self.store))
        self.assertEqual(self.store.counts(), {'success': 3})

    async def test_concurrent_workers_cancelled_and_drained_on_stop(self):
        self.add([message(10), message(9), message(8)])
        active = 0
        peak = 0
        started = asyncio.Event()

        async def worker(client, peer, source, store, record, state, decode=False):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                if record['message_id'] == 10:
                    await started.wait()
                    raise StopArchive('test_stop')
                started.set()
                await asyncio.Event().wait()
            finally:
                active -= 1

        with patch('video_archive.telegram.archive.download_one', side_effect=worker):
            with self.assertRaisesRegex(StopArchive, 'test_stop'):
                await asyncio.wait_for(archive.download_set(None, None, SOURCE, self.store, {}), 2)
        self.assertEqual(peak, 2)
        self.assertEqual(active, 0)

    async def test_duplicate_page_rejected_without_cursor_advance(self):
        client = FakeClient([message(10), message(10)])
        with self.assertRaisesRegex(StopArchive, 'pagination_boundary'):
            await archive.scan(client, None, self.store, 100)
        self.assertEqual(self.store.get('cursor'), 11)
        self.assertEqual(self.store.pagination_audit()['unique_ids'], 0)

    def test_page_transaction_rolls_back_records_and_cursor(self):
        good = record_message(message(10), SOURCE)
        broken = {'message_id': 9}
        with self.assertRaises(KeyError):
            self.store.ingest([good, broken], 11, 9, [10, 9])
        self.assertEqual(self.store.get('cursor'), 11)
        self.assertEqual(self.store.videos(), [])
        self.assertEqual(self.store.pagination_audit()['unique_ids'], 0)

    def test_user_bot_target_rejected_and_empty_label_not_inferred(self):
        with self.assertRaises(StopArchive):
            archive.source_info(types.User(id=123, bot=True))
        r = record_message(message(10, text='标题：\n仅是说明文字'), SOURCE)
        self.assertEqual(r['title'], '')

    def test_corrupt_container_and_fake_video_extension(self):
        path = self.store.root / 'bad.mp4'
        path.write_bytes(b'not a video')
        with self.assertRaisesRegex(ValueError, 'ffprobe_failed'):
            validate_file(path, path.stat().st_size)


if __name__ == '__main__':
    unittest.main(verbosity=2)