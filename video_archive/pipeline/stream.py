"""Private Cloudflare Stream TUS uploads and their separate encoding receipts."""
import base64
import json
import os
import re
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request, build_opener

from video_archive.pipeline.common import BASE, PipelineError
from video_archive.pipeline.storage import NoRedirect

CHUNK = 8 * 1024**2
UID = re.compile(r'[0-9a-f]{32}')


def stream_config(path=BASE / '.private/cloudflare.json'):
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise PipelineError('private_stream_config_missing')
    if path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077 or path.parent.stat().st_mode & 0o077:
        raise PipelineError('private_config_permissions')
    try:
        cfg = json.loads(path.read_text())
    except ValueError:
        raise PipelineError('stream_config_incomplete') from None
    if (not isinstance(cfg, dict) or not UID.fullmatch(str(cfg.get('account_id') or ''))
            or not isinstance(cfg.get('api_token'), str) or not cfg['api_token'].strip()):
        raise PipelineError('stream_config_incomplete')
    return cfg


class Stream:
    def __init__(self, project):
        cfg = stream_config()
        self.account = cfg['account_id']
        self.base = f'https://api.cloudflare.com/client/v4/accounts/{self.account}/stream'
        self.token = cfg['api_token']
        self.project = project
        self.opener = build_opener(NoRedirect())

    def request(self, method, url, headers=None, data=None):
        parts = urlsplit(url)
        api = url == self.base or url.startswith((self.base + '/', self.base + '?'))
        upload_host = (parts.hostname or '').split('.')
        if (parts.scheme != 'https' or parts.username or parts.password or parts.port or parts.fragment
                or not (api or '.'.join(upload_host[-2:]) in ('videodelivery.net', 'cloudflarestream.com'))):
            raise PipelineError('untrusted_stream_url')
        headers = dict(headers or {})
        if api:
            headers['Authorization'] = 'Bearer ' + self.token
        return self.opener.open(Request(url, data=data, headers=headers, method=method), timeout=60)

    def api(self, suffix, method='GET'):
        try:
            with self.request(method, self.base + suffix) as response:
                body = json.load(response)
        except HTTPError as exc:
            if exc.code in (401, 403):
                raise PipelineError('stream_permission_unavailable') from None
            raise
        if body.get('success') is not True:
            raise PipelineError('stream_api_failed')
        return body['result']

    def identity(self, vid, sha):
        return {'video_id': str(vid), 'project_ref': self.project, 'source_sha256': sha}

    def verify(self, asset, vid, sha, uid=None):
        if (not UID.fullmatch(asset.get('uid') or '') or uid and asset['uid'] != uid
                or asset.get('creator') != f'{self.project}-{vid}'
                or asset.get('requireSignedURLs') is not True
                or any((asset.get('meta') or {}).get(k) != v for k, v in self.identity(vid, sha).items())):
            raise PipelineError('stream_identity_conflict')
        return asset

    def create_upload(self, vid, size, sha, lease):
        creator = f'{self.project}-{vid}'
        metadata = {**self.identity(vid, sha), 'name': creator}
        encoded = ','.join(k + ' ' + base64.b64encode(v.encode()).decode() for k, v in metadata.items())
        headers = {'Tus-Resumable': '1.0.0', 'Upload-Length': str(size), 'Upload-Creator': creator,
                   'Upload-Metadata': encoded + ',requiresignedurls'}
        try:
            with self.request('POST', self.base, headers) as response:
                uid = response.headers.get('stream-media-id', '')
                location = response.headers.get('Location')
                if not UID.fullmatch(uid) or not location:
                    raise PipelineError('stream_create_receipt_missing')
                return uid, urljoin(self.base, location)
        except HTTPError as exc:
            if exc.code in (400, 401, 403, 413, 429):
                lease.db.call('stream_create_intent', vid, lease.token, False)
            if exc.code in (401, 403):
                raise PipelineError('stream_permission_unavailable') from None
            raise

    def upload(self, vid, path, sha, cache, lease, check):
        """Reserve creation before POST. An ambiguous create never authorizes a second POST."""
        size = path.stat().st_size
        journal = cache.get(vid).get('stream_session') or {}
        if journal and (journal['account'] != self.account or journal['sha256'] != sha):
            raise PipelineError('stream_identity_conflict')
        uid, url = journal.get('uid'), journal.get('url')
        if not uid:
            check()
            if lease.db.call('stream_create_intent', vid, lease.token, True):
                uid, url = self.create_upload(vid, size, sha, lease)
            else:
                lease.fence()
                assets = self.api('?' + urlencode({'creator': f'{self.project}-{vid}'}))
                if len(assets) != 1:
                    raise PipelineError('stream_reconciliation_pending')
                uid = self.verify(assets[0], vid, sha)['uid']
            cache.put(vid, stream_session={'account': self.account, 'sha256': sha, 'uid': uid, 'url': url})
        tus = {'Tus-Resumable': '1.0.0'}
        if url:
            check()
            try:
                with self.request('HEAD', url, tus) as response:
                    offset = int(response.headers['Upload-Offset'])
                    if int(response.headers['Upload-Length']) != size or not 0 <= offset <= size:
                        raise PipelineError('stream_offset_conflict')
            except HTTPError as exc:
                if exc.code not in (404, 410):
                    raise
            else:
                with path.open('rb') as source:
                    source.seek(offset)
                    while offset < size:
                        check()
                        block = source.read(min(CHUNK, size - offset))
                        if not block:
                            raise PipelineError('stream_source_changed')
                        with self.request('PATCH', url, {**tus, 'Content-Type': 'application/offset+octet-stream',
                                          'Upload-Offset': str(offset)}, block) as response:
                            offset += len(block)
                            if int(response.headers['Upload-Offset']) != offset:
                                raise PipelineError('stream_offset_conflict')
                return uid
        # The URL was lost or has expired. Reuse a completed upload only.
        asset = self.verify(self.api('/' + uid), vid, sha, uid)
        if asset['status']['state'] not in ('queued', 'inprogress', 'ready', 'error'):
            raise PipelineError('stream_reconciliation_pending')
        return uid

    def wait_ready(self, vid, upload, check):
        uid = upload['uploaded_stream_uid']
        deadline = time.monotonic() + 300
        while True:
            check()
            try:
                asset = self.verify(self.api('/' + uid), vid, upload['source_sha256'], uid)
            except HTTPError as exc:
                if exc.code != 429 and exc.code < 500:
                    raise
            except (URLError, TimeoutError, ConnectionError):
                pass  # A status read can be retried on this lease without uploading again.
            else:
                status = asset['status']
                if status['state'] == 'error':
                    raise PipelineError('stream_encoding_failed')
                if (status['state'] == 'ready' and asset.get('readyToStream') is True
                        and float(status.get('pctComplete') or 0) == 100):
                    return {'uid': uid, 'requireSignedURLs': True, 'readyToStream': True,
                            'state': 'ready', 'pctComplete': 100}
            # Bound a single polling lease; leave long encodes pending for the next pass.
            if time.monotonic() >= deadline:
                return None
            for _ in range(10):
                check()
                time.sleep(1)

    def ai_media(self, uid):
        """Only for a cache miss. Let ffmpeg seek the private encode without staging a second original."""
        asset = self.api('/' + uid)
        url = urlsplit(asset['playback']['hls'])
        if (asset.get('uid') != uid or asset.get('requireSignedURLs') is not True
                or url.scheme != 'https' or not (url.hostname or '').endswith(('.cloudflarestream.com', '.videodelivery.net'))
                or url.path != f'/{uid}/manifest/video.m3u8'):
            raise PipelineError('stream_playback_conflict')
        token = self.api('/' + uid + '/token', 'POST')['token']
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', token):
            raise PipelineError('stream_token_invalid')
        return f'https://{url.hostname}/{token}/manifest/video.m3u8'
