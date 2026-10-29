"""Authorize a fresh named session without modifying the active session database.

Use Telegram's login-token flow. The existing authorization is read into memory
only for accepting the token. The new session negotiates its own authorization
key; no session database, entity cache or authorization key is cloned to it.
"""
import argparse
import asyncio
import fcntl
import getpass
from pathlib import Path
import sqlite3
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from telethon import errors, functions
from telethon.crypto import AuthKey
from telethon.sessions import MemorySession

from video_archive.core import StopArchive
from video_archive.telegram.archive import (
    client_for, load_config, private_init, rpc, safe_error, session_paths,
)


def read_authorization(path):
    """Read one committed SQLite snapshot; never open the source for writing."""
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    try:
        row = connection.execute('SELECT dc_id,server_address,port,auth_key FROM sessions').fetchone()
    finally:
        connection.close()
    if not row or not row[3]:
        raise StopArchive('source_session_not_authorized')
    memory = MemorySession()
    memory.set_dc(row[0], row[1], row[2])
    memory.auth_key = AuthKey(data=row[3])
    return memory


async def authorize(name, source_name='default', *, interactive=False):
    if name == source_name:
        raise StopArchive('new_session_name_must_differ_from_source')
    source_path, _ = session_paths(source_name)
    memory = read_authorization(source_path.with_suffix('.session'))
    config = load_config()
    source = client_for(config, session=memory)
    target = client_for(config, session_name=name, receive_updates=True)
    waiter = None
    try:
        # Only copy the public DC endpoint. The target creates its own auth key.
        if target.session.auth_key and target.session.auth_key.key == memory.auth_key.key:
            raise StopArchive('target_reuses_source_authorization_key')
        if not target.session.auth_key:
            target.session.set_dc(memory.dc_id, memory.server_address, memory.port)
        await rpc(source.connect)
        owner = await rpc(source.get_me)
        if not owner or owner.bot:
            raise StopArchive('authorized_user_source_required')
        await rpc(target.connect)
        user = await rpc(target.get_me)
        if user is None:
            qr = await rpc(target.qr_login)
            waiter = asyncio.create_task(qr.wait(timeout=25))
            # Register QRLogin's update handler before accepting the token.
            await asyncio.sleep(0)
            await rpc(lambda: source(functions.auth.AcceptLoginTokenRequest(qr.token)))
            try:
                user = await waiter
            except errors.SessionPasswordNeededError:
                if not interactive or not sys.stdin.isatty():
                    raise StopArchive('two_factor_password_required: rerun this command with --interactive in a local terminal') from None
                password = getpass.getpass('Telegram 二步验证密码（仅在本机使用，隐藏输入）: ')
                try:
                    user = await rpc(lambda: target.sign_in(password=password))
                finally:
                    password = None
        if user.id != owner.id or user.bot:
            raise StopArchive('isolated_session_account_mismatch')
        if target.session.auth_key.key == memory.auth_key.key:
            raise StopArchive('isolated_session_key_not_independent')
        target.session.save()
        print(f'Independent session authorized: {name}; account matched; authorization key is different.', flush=True)
    finally:
        if waiter is not None and not waiter.done():
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        for client in (target, source):
            if client.is_connected():
                await asyncio.wait_for(client.disconnect(), timeout=20)
            else:
                client.session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--name', required=True)
    parser.add_argument('--source-session', default='default')
    parser.add_argument('--interactive', action='store_true', help='allow hidden local-terminal input for two-factor authentication')
    args = parser.parse_args()
    private_init()
    if args.name == 'default':
        parser.error('use a new named session, not the active default')
    _, lock_path = session_paths(args.name)
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            asyncio.run(authorize(args.name, args.source_session, interactive=args.interactive))
        except Exception as exc:
            print('Session setup stopped: ' + safe_error(exc), file=sys.stderr)
            return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
