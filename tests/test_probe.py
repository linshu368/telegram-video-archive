import json
import unittest
from unittest.mock import AsyncMock, patch

import test_archive as fixtures
from test_archive import FakeClient, SOURCE, message
import probe_download
from core import StopArchive, now


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    setUpClass = classmethod(fixtures.ArchiveTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ArchiveTests.tearDownClass.__func__)
    setUp = fixtures.ArchiveTests.setUp
    tearDown = fixtures.ArchiveTests.tearDown

    async def test_explicit_probe_preserves_protection_and_downloads_only_one(self):
        client = FakeClient([message(10, len(self.content)), message(9, len(self.content))], self.content)
        client.is_user_authorized = AsyncMock(return_value=True)
        client.is_bot = AsyncMock(return_value=False)
        protected = {**SOURCE, 'protected': True}
        report = {'started_at': now(), 'pages_checked': 0, 'messages_checked': 0}
        with patch('probe_download.confirmed_target', new=AsyncMock(return_value=(None, protected))), \
                patch('probe_download.history_page', new=AsyncMock(return_value=(client.messages, []))):
            await probe_download.probe(client, self.store.root, report)
        self.assertEqual(client.downloads, 1)
        self.assertEqual(report['result'], 'SINGLE_VIDEO_VERIFIED')
        record = json.loads((self.store.root / 'message.json').read_text())
        self.assertTrue(record['protected'])
        self.assertTrue(record['download']['decoded_at'])

    async def test_server_rejection_does_not_retry_other_videos(self):
        client = FakeClient([message(10), message(9)])
        client.is_user_authorized = AsyncMock(return_value=True)
        client.is_bot = AsyncMock(return_value=False)
        client.download_media = AsyncMock(side_effect=fixtures.errors.ChatForwardsRestrictedError(request=None))
        report = {'started_at': now(), 'pages_checked': 0, 'messages_checked': 0}
        with patch('probe_download.confirmed_target', new=AsyncMock(return_value=(None, SOURCE))), \
                patch('probe_download.history_page', new=AsyncMock(return_value=(client.messages, []))):
            with self.assertRaises(fixtures.errors.ChatForwardsRestrictedError):
                await probe_download.probe(client, self.store.root, report)
        self.assertEqual(client.download_media.await_count, 1)
        self.assertEqual(report['selected_message_id'], 10)
        self.assertNotEqual(report['result'], 'SINGLE_VIDEO_VERIFIED')

    async def test_expiring_media_never_attempted(self):
        msg = message(10)
        msg.media.ttl_seconds = 60
        client = FakeClient([msg])
        client.is_user_authorized = AsyncMock(return_value=True)
        client.is_bot = AsyncMock(return_value=False)
        report = {'started_at': now(), 'pages_checked': 0, 'messages_checked': 0}
        pages = AsyncMock(side_effect=[([msg], []), ([], [])])
        with patch('probe_download.confirmed_target', new=AsyncMock(return_value=(None, SOURCE))), \
                patch('probe_download.history_page', new=pages):
            with self.assertRaises(StopArchive):
                await probe_download.probe(client, self.store.root, report)
        self.assertEqual(client.downloads, 0)

    async def test_invalid_page_stops_before_download(self):
        client = FakeClient([message(9), message(10)])
        client.is_user_authorized = AsyncMock(return_value=True)
        client.is_bot = AsyncMock(return_value=False)
        report = {'started_at': now(), 'pages_checked': 0, 'messages_checked': 0}
        with patch('probe_download.confirmed_target', new=AsyncMock(return_value=(None, SOURCE))), \
                patch('probe_download.history_page', new=AsyncMock(return_value=(client.messages, []))):
            with self.assertRaisesRegex(StopArchive, 'pagination_boundary_error'):
                await probe_download.probe(client, self.store.root, report)
        self.assertEqual(client.downloads, 0)

    async def test_unauthorized_session_never_resolves_source(self):
        client = FakeClient([])
        client.is_user_authorized = AsyncMock(return_value=False)
        with patch('probe_download.confirmed_target', new=AsyncMock()) as resolve:
            with self.assertRaisesRegex(StopArchive, 'authorized_user_session_required'):
                await probe_download.probe(client, self.store.root, {})
        resolve.assert_not_awaited()