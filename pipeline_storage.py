"""Supabase TUS 1.0: bounded blocks, no upsert, streamed authenticated readback."""
import base64
import hashlib
import json
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pipeline_common import PipelineError, cover_object_path, object_path

CHUNK = 6 * 1024 * 1024
VIDEOS = 'drama-videos'
# Covers are public: a signing round trip per thumbnail would dominate the time to
# first paint, and a public object is CDN- and browser-cacheable. Videos stay private.
COVERS = 'drama-covers'
COVER_LIMIT = 5 * 1024 * 1024
COVER_CACHE = 'max-age=31536000, immutable'


def missing(exc):
    if exc.code == 404:
        return True
    # Storage versions may wrap not-found as HTTP 400. Auth errors never count.
    if exc.code == 400:
        try:
            data = json.loads(exc.read(8192))
            return str(data.get('statusCode')) == '404' or data.get('code') in ('NoSuchKey', 'NoSuchBucket')
        except (ValueError, AttributeError):
            pass
    return False


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise PipelineError('http_redirect_rejected')


class Storage:
    def __init__(self, c):
        self.base = f'https://{c["project_ref"]}.storage.supabase.co/storage/v1'
        self.key = c['storage_key']
        self.limit = min(c['global_file_limit'], c['bucket_file_limit'])
        self.cover_limit = min(COVER_LIMIT, c['global_file_limit'])
        self.opener = build_opener(NoRedirect())

    def request(self, method, url, body=None, headers=None):
        self.check_url(url)
        auth = {'apikey': self.key}
        # Legacy service_role JWT uses Bearer; new sb_secret keys use apikey only.
        if not self.key.startswith('sb_secret_'):
            auth['Authorization'] = f'Bearer {self.key}'
        req = Request(url, data=body, method=method, headers={**auth, **(headers or {})})
        # Retry only read-only request setup. POST/PATCH ambiguity is reconciled
        # through persisted TUS sessions and full readback on the next file pass.
        attempts = 3 if method in ('GET', 'HEAD') else 1
        for attempt in range(attempts):
            try:
                return self.opener.open(req, timeout=60)
            except HTTPError as exc:
                if exc.code in (401, 403):
                    raise PipelineError('storage_permission_unavailable') from None
                if exc.code < 500 or attempt == attempts - 1:
                    raise
                exc.close()
            except (URLError, TimeoutError, ConnectionError):
                if attempt == attempts - 1:
                    raise
            time.sleep(2 ** attempt)

    def check_url(self, url):
        p, base = urlsplit(url), urlsplit(self.base)
        if p.scheme != 'https' or p.netloc != base.netloc or p.query or p.fragment or not p.path.startswith('/storage/v1/'):
            raise PipelineError('untrusted_upload_url')

    def bucket(self, create=False, *, name=VIDEOS, public=False, limit=None):
        limit = self.limit if limit is None else limit
        url = self.base + '/bucket/' + name
        try:
            with self.request('GET', url) as response:
                info = json.load(response)
        except HTTPError as exc:
            if not missing(exc) or not create:
                raise PipelineError('bucket_unavailable') from None
            body = json.dumps({'id': name, 'name': name, 'public': public,
                               'file_size_limit': limit}).encode()
            with self.request('POST', self.base + '/bucket', body, {'Content-Type': 'application/json'}):
                pass
            return self.bucket(name=name, public=public, limit=limit)
        if info.get('public') is not public or info.get('file_size_limit') != limit or info.get('allowed_mime_types'):
            raise PipelineError('bucket_configuration_conflict')
        return {'name': name, 'private': not public, 'file_size_limit': info['file_size_limit']}

    def cover_bucket(self, create=False):
        return self.bucket(create, name=COVERS, public=True, limit=self.cover_limit)

    def remote_size(self, vid, check=lambda: None, *, bucket=VIDEOS, path=None):
        """Current size the object actually has, straight from the server.

        A locally cached size can be stale (independent of transcoding at all --
        e.g. a prior re-upload the cache never recorded), so the transcode task
        re-asks here rather than trusting a cached value that readback() would
        then use as ground truth.
        """
        check()
        try:
            with self.request('HEAD', f'{self.base}/object/authenticated/{bucket}/{path or object_path(vid)}') as response:
                return int(response.headers['Content-Length'])
        except HTTPError as exc:
            if missing(exc):
                return None
            raise PipelineError('remote_read_failed') from None

    def readback(self, vid, expected_size, check=lambda: None, output=None, *, metrics=None,
                 bucket=VIDEOS, path=None):
        began = time.monotonic()
        try:
            return self._readback(vid, expected_size, check, output, metrics, bucket,
                                  path or object_path(vid))
        finally:
            if metrics is not None:
                metrics['readback_seconds'] = round(metrics.get('readback_seconds', 0) + time.monotonic() - began, 3)

    def _readback(self, vid, expected_size, check, output, metrics, bucket, target):
        digest, total = hashlib.sha256(), 0
        check()
        try:
            # Authenticated read even for the public cover bucket: verification must
            # not depend on anonymous access or on a cached CDN copy.
            with self.request('GET', f'{self.base}/object/authenticated/{bucket}/{target}') as response:
                while True:
                    check()
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    total += len(block)
                    if metrics is not None:
                        metrics['readback_bytes'] = metrics.get('readback_bytes', 0) + len(block)
                    if total > expected_size:
                        raise PipelineError('remote_size_conflict')
                    digest.update(block)
                    if output:
                        output.write(block)
        except HTTPError as exc:
            if missing(exc):
                return None
            raise PipelineError('remote_read_failed') from None
        if total != expected_size:
            raise PipelineError('remote_size_conflict')
        return digest.hexdigest()

    def upload_cover(self, vid, path, expected_sha, check=lambda: None):
        """One POST, no TUS: a cover is small, immutable and cheap to repeat.

        Readback-first makes a crash between POST and DB commit reconcilable
        without ever trusting the local file as proof of what the bucket holds.
        """
        target = cover_object_path(vid)
        size = path.stat().st_size
        if size > self.cover_limit:
            raise PipelineError('cover_file_limit')
        actual = self.readback(vid, size, check, bucket=COVERS, path=target)
        if actual is not None:
            if actual != expected_sha:
                raise PipelineError('remote_cover_conflict')
            return actual
        check()
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != expected_sha:
            raise PipelineError('local_cover_changed')
        try:
            with self.request('POST', f'{self.base}/object/{COVERS}/{target}', body,
                              {'Content-Type': 'image/jpeg', 'Cache-Control': COVER_CACHE,
                               'x-upsert': 'false'}):
                pass
        except HTTPError as exc:
            # 409 means a previous attempt already stored it; the readback below decides.
            if exc.code != 409:
                raise PipelineError('cover_upload_failed') from None
        actual = self.readback(vid, size, check, bucket=COVERS, path=target)
        if actual != expected_sha:
            raise PipelineError('remote_cover_sha256_mismatch')
        return actual

    def upload(self, vid, path, expected_sha, cache, check=lambda: None, mime='video/mp4', *, metrics=None):
        size = path.stat().st_size
        if size > self.limit:
            raise PipelineError('bucket_file_limit')
        actual = self.readback(vid, size, check, metrics=metrics)
        if actual is not None:
            if actual != expected_sha:
                raise PipelineError('remote_object_conflict')
            return actual  # Uploaded before a crash/failed DB commit; do not POST again.
        entry = cache.get(vid)
        url = entry.get('upload_url') if entry.get('upload_sha256') == expected_sha else None
        tus = {'Tus-Resumable': '1.0.0'}
        offset = 0
        if url:
            self.check_url(url)
            try:
                with self.request('HEAD', url, headers=tus) as response:
                    offset = int(response.headers['Upload-Offset'])
                    if int(response.headers['Upload-Length']) != size:
                        raise PipelineError('upload_session_conflict')
            except HTTPError as exc:
                if exc.code not in (404, 410):
                    raise PipelineError('upload_resume_failed') from None
                url = None
        if not url:
            metadata = {'bucketName': VIDEOS, 'objectName': object_path(vid), 'contentType': mime}
            encoded = ','.join(k + ' ' + base64.b64encode(v.encode()).decode() for k, v in metadata.items())
            check()
            with self.request('POST', self.base + '/upload/resumable', b'',
                              {**tus, 'Upload-Length': str(size), 'Upload-Metadata': encoded, 'x-upsert': 'false'}) as response:
                url = urljoin(self.base + '/upload/resumable', response.headers['Location'])
                self.check_url(url)
                offset = int(response.headers.get('Upload-Offset', '0'))
            cache.put(vid, upload_url=url, upload_sha256=expected_sha)
        if not 0 <= offset <= size:
            raise PipelineError('upload_offset_conflict')
        with path.open('rb') as handle:
            handle.seek(offset)
            while offset < size:
                check()
                block = handle.read(min(CHUNK, size - offset))
                if not block:
                    raise PipelineError('local_file_changed')
                began = time.monotonic()
                try:
                    with self.request('PATCH', url, block, {**tus, 'Upload-Offset': str(offset),
                                      'Content-Type': 'application/offset+octet-stream'}) as response:
                        next_offset = int(response.headers['Upload-Offset'])
                finally:
                    if metrics is not None:
                        metrics['tus_patch_seconds'] = round(metrics.get('tus_patch_seconds', 0) + time.monotonic() - began, 3)
                if next_offset != offset + len(block):
                    raise PipelineError('upload_offset_conflict')
                offset = next_offset
                if metrics is not None:
                    metrics['tus_uploaded_bytes'] = metrics.get('tus_uploaded_bytes', 0) + len(block)
        actual = self.readback(vid, size, check, metrics=metrics)
        if actual != expected_sha:
            raise PipelineError('remote_sha256_mismatch')
        return actual

    def replace(self, vid, path, expected_sha, check=lambda: None, mime='video/mp4', *, metrics=None):
        """Overwrite the existing object at the same path with different bytes.

        Only for a controlled, identity-preserving rewrite (lossless remux of an
        object this same video already owns). upload() refuses this on purpose
        (x-upsert: false, 'remote_object_conflict' on a hash mismatch) to guard
        against accidentally clobbering a different video's data; this method
        is the one place that guard is deliberately lifted.

        Not resumable: Supabase's TUS resumable endpoint rejects a PATCH into an
        object that already exists ('The resource already exists', regardless
        of x-upsert), so this uses the plain object POST endpoint's upsert
        instead -- one whole-body request, retried from scratch by the caller's
        own task retry on failure rather than resumed from a byte offset.
        """
        size = path.stat().st_size
        if size > self.limit:
            raise PipelineError('bucket_file_limit')
        # Faststart remuxing is not always byte-for-byte size-preserving (moving the
        # moov atom can grow or shrink the file by a few KB), so the pre-replace
        # object is very often a different size than the new local file. That is
        # the expected starting state here, not a conflict -- only a genuine crash
        # recovery (remote already equals the new content) should short-circuit.
        try:
            actual = self.readback(vid, size, check, metrics=metrics)
        except PipelineError as exc:
            if str(exc) != 'remote_size_conflict':
                raise
            actual = None
        if actual == expected_sha:
            return actual  # Already replaced before a crash/failed DB commit; do not POST again.
        check()
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != expected_sha:
            raise PipelineError('local_file_changed')
        began = time.monotonic()
        try:
            with self.request('POST', f'{self.base}/object/{VIDEOS}/{object_path(vid)}', body,
                              {'Content-Type': mime, 'x-upsert': 'true'}):
                pass
        finally:
            if metrics is not None:
                metrics['replace_post_seconds'] = round(metrics.get('replace_post_seconds', 0) + time.monotonic() - began, 3)
        # Overwriting an existing object (unlike a brand-new upload) can briefly
        # read back the pre-overwrite bytes right after a successful write --
        # there is an old version (a different size, or the same size but old
        # content) for a lagging read to still catch. Short, bounded retries
        # absorb that propagation lag instead of failing a replace that
        # actually succeeded.
        for attempt in range(4):
            try:
                actual = self.readback(vid, size, check, metrics=metrics)
            except PipelineError as exc:
                if str(exc) != 'remote_size_conflict' or attempt == 3:
                    raise
                actual = None
            if actual == expected_sha:
                return actual
            if attempt < 3:
                time.sleep(2 ** attempt)
        raise PipelineError('remote_sha256_mismatch')
