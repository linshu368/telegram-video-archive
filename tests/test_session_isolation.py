"""Separate session files, locks and authorization keys, without Telegram IO."""
import asyncio
import fcntl
import hashlib
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from telethon import errors
from telethon.crypto import AuthKey
from telethon.sessions import MemorySession, SQLiteSession

from scripts.diagnostics import create_telegram_session as setup
from video_archive.core import StopArchive
from video_archive.telegram import archive


class SessionPathsTests(unittest.TestCase):
    def test_default_keeps_existing_paths_and_named_session_has_an_independent_lock(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(archive, 'PRIVATE', Path(tmp)):
            default, default_lock = archive.session_paths()
            named, named_lock = archive.session_paths('download-audit')
            self.assertEqual(default, Path(tmp) / 'user')
            self.assertEqual(default_lock, Path(tmp) / 'process.lock')
            self.assertEqual(named, Path(tmp) / 'sessions/download-audit/user')
            with default_lock.open('a') as first, named_lock.open('a') as second:
                fcntl.flock(first, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(second, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with named_lock.open('a') as duplicate:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(duplicate, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_session_names_cannot_escape_private_directory(self):
        for name in ('../other', '/tmp/user', '', 'a/b'):
            with self.assertRaisesRegex(StopArchive, 'invalid_session_name'):
                archive.session_paths(name)

    def test_client_uses_selected_session_and_bootstrap_can_stay_in_memory(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(archive, 'PRIVATE', Path(tmp)), \
                patch.object(archive, 'TelegramClient') as factory:
            config = {'api_id': 123, 'api_hash': 'test'}
            archive.client_for(config, session_name='audit')
            self.assertEqual(factory.call_args.args[0], str(Path(tmp) / 'sessions/audit/user'))
            memory = MemorySession()
            archive.client_for(config, session=memory)
            self.assertIs(factory.call_args.args[0], memory)
            self.assertFalse((Path(tmp) / 'user.session').exists())

    def test_loading_source_authorization_does_not_write_or_copy_the_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'original.session'
            source = SQLiteSession(str(path))
            source.set_dc(1, '127.0.0.1', 443)
            source.auth_key = AuthKey(b'a' * 256)
            source.save()
            source.close()
            before = hashlib.sha256(path.read_bytes()).digest()
            memory = setup.read_authorization(path)
            self.assertIsInstance(memory, MemorySession)
            self.assertEqual(memory.auth_key.key, b'a' * 256)
            self.assertEqual(hashlib.sha256(path.read_bytes()).digest(), before)
            self.assertEqual(list(Path(tmp).iterdir()), [path])


class AuthorizationTests(unittest.IsolatedAsyncioTestCase):
    def clients(self, same_key=False):
        memory = MemorySession()
        memory.set_dc(1, '127.0.0.1', 443)
        memory.auth_key = AuthKey(b'a' * 256)
        owner = SimpleNamespace(id=123, bot=False)
        accepted = asyncio.Event()

        class Client:
            def __init__(self, session, user):
                self.session = session
                self.connect = AsyncMock()
                self.disconnect = AsyncMock()
                self.get_me = AsyncMock(return_value=user)
                self.qr_login = AsyncMock(return_value=self)
                self.token = b'local-test-token'

            def is_connected(self):
                return True

            async def wait(self, timeout):
                await accepted.wait()
                return owner

            async def __call__(self, request):
                accepted.set()

        target_memory = MemorySession()
        target_memory.auth_key = AuthKey((b'a' if same_key else b'b') * 256)
        return memory, Client(memory, owner), Client(target_memory, None)

    async def test_login_token_flow_leaves_target_with_an_independent_key(self):
        memory, source, target = self.clients()
        with (patch.object(setup, 'session_paths', return_value=(Path('/unused/user'), Path('/unused/lock'))),
              patch.object(setup, 'read_authorization', return_value=memory),
              patch.object(setup, 'load_config', return_value={}),
              patch.object(setup, 'client_for', side_effect=[source, target])):
            await setup.authorize('download-audit')
        self.assertNotEqual(source.session.auth_key.key, target.session.auth_key.key)
        source.connect.assert_awaited_once()
        target.connect.assert_awaited_once()
        source.disconnect.assert_awaited_once()
        target.disconnect.assert_awaited_once()

    async def test_cloned_authorization_is_rejected_before_network_access(self):
        memory, source, target = self.clients(same_key=True)
        with (patch.object(setup, 'session_paths', return_value=(Path('/unused/user'), Path('/unused/lock'))),
              patch.object(setup, 'read_authorization', return_value=memory),
              patch.object(setup, 'load_config', return_value={}),
              patch.object(setup, 'client_for', side_effect=[source, target])):
            with self.assertRaisesRegex(StopArchive, 'target_reuses_source_authorization_key'):
                await setup.authorize('download-audit')
        source.connect.assert_not_awaited()
        target.connect.assert_not_awaited()

    async def test_two_factor_requires_hidden_local_input_and_completes_same_login(self):
        memory, source, target = self.clients()
        target.wait = AsyncMock(side_effect=errors.SessionPasswordNeededError(request=None))
        target.sign_in = AsyncMock(return_value=source.get_me.return_value)
        with (patch.object(setup, 'session_paths', return_value=(Path('/unused/user'), Path('/unused/lock'))),
              patch.object(setup, 'read_authorization', return_value=memory),
              patch.object(setup, 'load_config', return_value={}),
              patch.object(setup, 'client_for', side_effect=[source, target]),
              patch.object(setup.sys.stdin, 'isatty', return_value=True),
              patch.object(setup.getpass, 'getpass', return_value='test-only-password') as hidden):
            await setup.authorize('download-audit', interactive=True)
        hidden.assert_called_once()
        target.sign_in.assert_awaited_once_with(password='test-only-password')

    async def test_two_factor_never_prompts_in_background_process(self):
        memory, source, target = self.clients()
        target.wait = AsyncMock(side_effect=errors.SessionPasswordNeededError(request=None))
        with (patch.object(setup, 'session_paths', return_value=(Path('/unused/user'), Path('/unused/lock'))),
              patch.object(setup, 'read_authorization', return_value=memory),
              patch.object(setup, 'load_config', return_value={}),
              patch.object(setup, 'client_for', side_effect=[source, target]),
              patch.object(setup.getpass, 'getpass') as hidden):
            with self.assertRaisesRegex(StopArchive, 'two_factor_password_required'):
                await setup.authorize('download-audit')
        hidden.assert_not_called()


if __name__ == '__main__':
    unittest.main()
