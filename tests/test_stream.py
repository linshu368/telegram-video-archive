"""Stream transfer recovery, private encoding evidence, and downstream availability."""
import hashlib
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from video_archive.pipeline import runner, workflow
from video_archive.pipeline.cache import Cache
from video_archive.pipeline.common import PipelineError
from video_archive.pipeline.completion import _delivery
from video_archive.pipeline.stream import Stream
from test_workflow import FakeDB, jobs, PROJECT


class StreamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cache = Cache(self.root)
        self.rows = jobs(2)
        self.vid = self.rows[0][1]['id']
        self.uid = 'a1' * 16
        self.path = self.root / 'original.mp4'
        self.path.write_bytes(b'a valid test payload')
        self.sha = hashlib.sha256(self.path.read_bytes()).hexdigest()
        self.cache.put(self.vid, sha256=self.sha, path=str(self.path))
        cfg = {'account_id': 'b1'*16, 'api_token': 'test'}
        with patch('video_archive.pipeline.stream.stream_config', return_value=cfg):
            self.client = Stream(PROJECT)
        self.asset = {'uid': self.uid, 'creator': f'{PROJECT}-{self.vid}', 'requireSignedURLs': True,
                      'meta': self.client.identity(self.vid, self.sha), 'readyToStream': True,
                      'status': {'state': 'ready', 'pctComplete': '100.0000'}}
        self.lease = Mock()
        self.lease.db.call.return_value = True
        self.db = FakeDB(self.rows, ai=True)
        self.state = self.db.states[self.vid]
        self.state.update(storage_provider='cloudflare_stream', object_path=None, stream_uid=None,
                          stream_ready_at=None)
        self.upload = self.state['tasks']['upload']
        self.upload.update(uploaded_stream_uid=self.uid, source_sha256=self.sha,
                           source_bytes=self.path.stat().st_size, stream_create_started=True)
        self.state['tasks']['stream_process'] = {'status': 'pending'}

    def transfer(self):
        return self.client.upload(self.vid, self.path, self.sha, self.cache, self.lease, lambda: None)

    def test_lost_patch_response_uses_offset_without_a_second_create_or_patch(self):
        calls, content = [], bytearray()

        @contextmanager
        def request(method, url, headers, data=None):
            calls.append(method)
            if method == 'POST':
                self.assertIn('requiresignedurls', headers['Upload-Metadata'].split(','))
                self.assertIn('video_id ', headers['Upload-Metadata'])
                self.assertEqual(headers['Upload-Creator'], self.asset['creator'])
                reply = {'stream-media-id': self.uid, 'Location': 'https://upload.videodelivery.net/session'}
            elif method == 'HEAD':
                reply = {'Upload-Offset': str(len(content)), 'Upload-Length': str(self.path.stat().st_size)}
            else:
                content.extend(data)
                raise ConnectionError('PATCH response lost')
            yield SimpleNamespace(headers=reply)

        self.client.request = request
        with self.assertRaises(ConnectionError):
            self.transfer()
        self.assertEqual(self.transfer(), self.uid)
        self.assertEqual(bytes(content), self.path.read_bytes())
        self.assertEqual(calls, ['POST', 'HEAD', 'PATCH', 'HEAD'])

    def test_missing_journal_reconciles_remote_identity_or_stays_pending(self):
        self.lease.db.call.return_value = False
        self.client.request = Mock(side_effect=AssertionError('unexpected TUS create'))
        self.client.api = Mock(side_effect=[[], [self.asset, self.asset], [self.asset], self.asset])
        for _ in range(2):
            with self.assertRaisesRegex(PipelineError, 'reconciliation_pending'):
                self.transfer()
        self.assertEqual(self.transfer(), self.uid)
        self.client.request.assert_not_called()

    def test_ready_requires_private_identity_and_every_quality_level(self):
        first = {**self.asset, 'status': {'state': 'ready', 'pctComplete': '39'}}
        self.client.api = Mock(side_effect=[TimeoutError(), first, self.asset])
        with patch('video_archive.pipeline.stream.time.sleep'):
            receipt = self.client.wait_ready(self.vid, self.upload, lambda: None)
        self.assertEqual((receipt['uid'], receipt['pctComplete']), (self.uid, 100))
        for change, code in (({'requireSignedURLs': False}, 'identity_conflict'),
                             ({'meta': {}}, 'identity_conflict'),
                             ({'status': {'state': 'error'}}, 'encoding_failed')):
            self.client.api = Mock(return_value={**self.asset, **change})
            with self.assertRaisesRegex(PipelineError, code):
                self.client.wait_ready(self.vid, self.upload, lambda: None)

    def test_long_encoding_returns_pending_without_success_evidence(self):
        self.client.api = Mock(return_value={**self.asset, 'readyToStream': False})
        with patch('video_archive.pipeline.stream.time.monotonic', side_effect=[0, 301]):
            self.assertIsNone(self.client.wait_ready(self.vid, self.upload, lambda: None))

    def test_ai_fallback_requests_one_token_for_a_trusted_private_asset(self):
        asset = {**self.asset, 'playback': {'hls': f'https://customer-test.cloudflarestream.com/{self.uid}/manifest/video.m3u8'}}
        self.client.api = Mock(side_effect=[asset, {'token': 'signed.test.token'}])
        url = self.client.ai_media(self.uid)
        self.assertEqual(url, 'https://customer-test.cloudflarestream.com/signed.test.token/manifest/video.m3u8')
        self.assertEqual(self.client.api.call_args.args, ('/' + self.uid + '/token', 'POST'))
        self.client.api = Mock(return_value={**asset, 'playback': {'hls': 'https://untrusted.example/video'}})
        with self.assertRaisesRegex(PipelineError, 'playback_conflict'):
            self.client.ai_media(self.uid)
        self.assertEqual(self.client.api.call_count, 1)

    def test_cache_ai_and_drama_gate_before_and_after_ready(self):
        with self.assertRaisesRegex(PipelineError, 'not_committed'):
            self.cache.cleanup(self.vid, self.db)
        with self.assertRaisesRegex(PipelineError, 'stream_not_ready'), runner.ai_input(
                self.db, None, self.cache, self.vid, lambda: None):
            pass
        self.assertTrue(self.path.exists())
        self.assertNotIn(self.rows[0][2]['message_id'], runner.committed_inventory(self.db, self.rows)[0])
        result = workflow.finalize_processed(self.db, self.rows, path=self.root/'pool.json', project_ref=PROJECT)
        self.assertEqual(result[0]['reason'], 'stream_processing')
        evaluation = {'delivery_finalized': True, 'decision': 'accept', 'planning_status': 'scheduled'}
        self.assertEqual(_delivery(evaluation, set(range(20)), set(range(1, 20)),
                                   {0: {'result': 'stream_processing'}}, True)['delivery_decision'], 'pending')
        self.state.update(stream_uid=self.uid, stream_ready_at='2026-01-01T00:00:00Z')
        self.state['tasks']['stream_process']['status'] = 'succeeded'
        with runner.ai_input(self.db, None, self.cache, self.vid, lambda: None) as path:
            self.assertEqual(path, self.path)
        self.cache.cleanup(self.vid, self.db)
        self.assertFalse(self.path.exists())
        with patch.object(runner, 'Stream') as stream:
            stream.return_value.ai_media.return_value = 'private AI input'
            with runner.ai_input(self.db, None, self.cache, self.vid, lambda: None) as media:
                self.assertEqual(media, 'private AI input')
            stream.return_value.ai_media.assert_called_once_with(self.uid)
        self.assertIn(self.rows[0][2]['message_id'], runner.committed_inventory(self.db, self.rows)[0])
