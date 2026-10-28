"""The metadata-only entry point never starts a media transfer or verification."""
import asyncio
import io
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import video_archive.telegram.archive as archive
from video_archive.core import Store, StopArchive, record_message
from telethon import types


SOURCE = {'id': -1000000000123, 'bare_id': 123, 'name': 'test', 'username': '',
          'type': 'channel', 'forum': False, 'protected': False}
DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)


def video(mid):
    document = types.Document(
        id=9000 + mid, access_hash=123, file_reference=b'test-only', date=DATE,
        mime_type='video/mp4', size=2_500_000, dc_id=2,
        attributes=[types.DocumentAttributeFilename('sample.mp4'),
                    types.DocumentAttributeVideo(duration=10, w=720, h=1280)])
    return types.Message(id=mid, peer_id=types.PeerChannel(123), date=DATE,
                         message='', media=types.MessageMediaDocument(document=document))


class MetadataClient:
    def __init__(self, messages):
        self.messages = messages
        self.requests = []
        self.connected = False

    async def connect(self):
        self.connected = True

    def is_connected(self):
        return self.connected

    async def disconnect(self):
        self.connected = False

    async def is_user_authorized(self):
        return True

    async def is_bot(self):
        return False

    async def __call__(self, request):
        self.requests.append(request)
        messages = sorted(self.messages, key=lambda m: m.id, reverse=True)
        if request.offset_id:
            messages = [m for m in messages if m.id < request.offset_id]
        if request.max_id:
            messages = [m for m in messages if m.id < request.max_id]
        if request.offset_date:
            messages = [m for m in messages if m.date < request.offset_date]
        return types.messages.Messages(messages=messages[:min(request.limit, 2)],
                                       topics=[], users=[], chats=[])

    async def download_media(self, *args, **kwargs):
        raise AssertionError('metadata scan must not download media')


def dependencies(stack, client, store=None):
    stack.enter_context(patch('video_archive.telegram.archive.load_config', return_value={'test': True}))
    stack.enter_context(patch('video_archive.telegram.archive.client_for', return_value=client))
    stack.enter_context(patch('video_archive.telegram.archive.confirmed_target', new=AsyncMock(return_value=('peer', SOURCE))))
    stack.enter_context(patch('video_archive.telegram.archive.disk_check', return_value=1024**4))
    async def source_inventory(client, peer, source, store, **kwargs):
        with store.db:
            store.set('source_topic_inventory_complete', True)
        return {'completed_topic_ids': []}
    stack.enter_context(patch('video_archive.screening.inventory.collect_source_inventory',
                              new=AsyncMock(side_effect=source_inventory)))
    forbidden = {
        'media_tools': stack.enter_context(patch('video_archive.telegram.archive.media_tools')),
        'download_set': stack.enter_context(patch('video_archive.telegram.archive.download_set', new=AsyncMock())),
        'verify': stack.enter_context(patch('video_archive.telegram.archive.verify')),
        'choose_sample': stack.enter_context(patch('video_archive.telegram.archive.choose_sample')),
    }
    if store is not None:
        forbidden['prepare_downloads'] = stack.enter_context(patch.object(store, 'prepare_downloads'))
    output = stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
    return forbidden, output


class MetadataScanTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_invalidates_old_proof_before_configuration_failure(self):
        with self.store.db:
            self.store.set('source_topic_inventory_complete', True)
            self.store.set('source_topic_ids', [10])
        args = archive.parser().parse_args(['scan'])
        with patch('video_archive.telegram.archive.load_config', side_effect=StopArchive('missing_config')):
            with self.assertRaisesRegex(StopArchive, 'missing_config'):
                await archive.execute(args, self.store)
        self.assertFalse(self.store.get('source_topic_inventory_complete'))
        self.assertEqual(self.store.get('source_topic_ids'), [])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store(self.root)

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def args(self):
        return archive.parser().parse_args(['--data', str(self.root), 'scan', '--page-size', '2'])

    async def test_fresh_scan_collects_full_fixed_boundary_without_media(self):
        client = MetadataClient([video(3), video(2), video(1)])
        with ExitStack() as stack:
            forbidden, output = dependencies(stack, client, self.store)
            result = await archive.execute(self.args(), self.store)
            for mock in forbidden.values():
                mock.assert_not_called()
        self.assertEqual(result, 0)
        self.assertTrue(self.store.get('scan_complete'))
        self.assertEqual(self.store.get('upper_id'), 3)
        self.assertEqual(len(self.store.videos()), 3)
        self.assertTrue(all(state['status'] == 'pending' for _, state in self.store.videos()))
        self.assertTrue(all(record['width'] == 720 and record['height'] == 1280
                            and record['duration_seconds'] == 10
                            for record, _ in self.store.videos()))
        operation = self.store.get('last_operation')
        self.assertTrue(operation['metadata_scan_complete'])
        self.assertFalse(operation['media_downloaded'])
        self.assertEqual(operation['downloaded_success'], 0)
        self.assertFalse(self.store.get('preflight')['download_allowed'])
        self.assertIn('metadata_scan_complete', output.getvalue())
        self.assertFalse(client.connected)

    async def test_resume_preserves_existing_video_states_and_frozen_boundary(self):
        self.store.start(SOURCE, '2025-01-02T00:00:00+00:00', 10)
        with self.store.db:
            self.store.set('cutoff_exclusive_utc', '2025-01-02T00:00:00+00:00')
            self.store.set('last_verification', {'test': 'preserve'})
            self.store.set('sample_passed', True)
        records = [record_message(video(mid), SOURCE) for mid in (9, 8)]
        self.store.ingest(records, 11, 8, [9, 8])
        for record, state in self.store.videos():
            state.update(status='blocked' if record['message_id'] == 9 else 'success',
                         failure_reason='protected_source', attempts=3, sha256='existing')
            self.store.update(record['message_id'], state)
        before = dict(self.store.db.execute('SELECT message_id,data FROM videos'))
        covers_before = dict(self.store.db.execute('SELECT message_id,data FROM covers'))
        client = MetadataClient([video(9), video(8), video(7)])
        with ExitStack() as stack:
            forbidden, _ = dependencies(stack, client, self.store)
            self.assertEqual(await archive.execute(self.args(), self.store), 0)
            for mock in forbidden.values():
                mock.assert_not_called()
        after = dict(self.store.db.execute('SELECT message_id,data FROM videos'))
        covers_after = dict(self.store.db.execute('SELECT message_id,data FROM covers'))
        self.assertEqual({mid: after[mid] for mid in before}, before)
        self.assertEqual({mid: covers_after[mid] for mid in covers_before}, covers_before)
        self.assertEqual(self.store.get('upper_id'), 10)
        self.assertEqual(self.store.get('started_at'), '2025-01-02T00:00:00+00:00')
        self.assertEqual(self.store.get('last_verification'), {'test': 'preserve'})
        self.assertTrue(self.store.get('sample_passed'))
        self.assertEqual([r.offset_id for r in client.requests], [8, 7])
        self.assertTrue(all(r.max_id == 11 for r in client.requests))
        self.assertEqual(json.loads(after[7])['status'], 'pending')
        self.assertTrue(self.store.get('scan_complete'))

    async def test_scan_does_not_claim_success_before_history_end(self):
        client = MetadataClient([])
        with ExitStack() as stack:
            forbidden, _ = dependencies(stack, client, self.store)
            stack.enter_context(patch('video_archive.telegram.archive.scan', new=AsyncMock()))
            with self.assertRaisesRegex(StopArchive, 'metadata_scan_incomplete'):
                await archive.execute(self.args(), self.store)
            for mock in forbidden.values():
                mock.assert_not_called()
        self.assertFalse(self.store.get('scan_complete'))
        self.assertFalse(self.store.get('last_operation').get('metadata_scan_complete', False))


class MetadataScanMainTests(unittest.TestCase):
    def test_successful_scan_exit_zero_does_not_claim_video_archive_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private = root / 'private'
            private.mkdir()
            data = root / 'archive'
            client = MetadataClient([video(1)])
            with ExitStack() as stack:
                forbidden, output = dependencies(stack, client)
                stack.enter_context(patch('video_archive.telegram.archive.PRIVATE', private))
                stack.enter_context(patch.object(sys, 'argv', ['archive.py', '--data', str(data), 'scan']))
                self.assertEqual(archive.main(), 0)
                for mock in forbidden.values():
                    mock.assert_not_called()
            report = json.loads((data / 'report.json').read_text())
            self.assertEqual(report['result'], 'INCOMPLETE')
            self.assertTrue(report['reached_api_history_end'])
            self.assertTrue(report['last_operation']['metadata_scan_complete'])
            self.assertEqual(report['video_states_mutually_exclusive']['pending'], 1)
            self.assertIn('metadata_scan_complete', output.getvalue())
            self.assertIn('result=INCOMPLETE', output.getvalue())


if __name__ == '__main__':
    unittest.main()
