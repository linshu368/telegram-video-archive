"""Exercise the public connected entry point with fake external services."""
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from video_archive.pipeline import runner
from video_archive.pipeline.common import PipelineError, snapshot
from video_archive.pipeline.outcomes import write_outcomes
from video_archive.pipeline.processed import load_pool, processed_file
from test_drama_pipeline_cli import source_inventory
from test_download_repair import committed


class ConnectedWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = {'project_ref': 'testref', 'database_dsn': 'unused',
                       'global_file_limit': 128 * 1024**2, 'bucket_file_limit': 128 * 1024**2}
        self.db, self.storage = MagicMock(), MagicMock()
        self.storage.limit = 128 * 1024**2
        self.cloud, self.events, self.downloads = {}, [], []
        self.db.archived_messages.side_effect = lambda: {
            int(state['message_id']) for state in self.cloud.values()}
        self.db.state.side_effect = lambda vid: self.cloud.get(vid)
        self.fail_ai = False

    def tearDown(self):
        self.temp.cleanup()

    def scan(self, root, **kwargs):
        self.events.append('scan')
        if not (root / 'state.sqlite3').exists():
            source_inventory(root)

    async def download(self, c, db, storage, cache, store, jobs, **kwargs):
        self.events.append('download')
        self.downloads.append([job[2]['message_id'] for job in jobs])
        for job in jobs:
            self.cloud[job[1]['id']] = committed(job)
        evaluations = kwargs['drama_evaluations']
        for evaluation in evaluations:
            if evaluation['decision'] == 'accept':
                evaluation.update(delivery_accepted=True, delivery_finalized=True,
                                  delivery_decision='complete', cover_complete=True)
        write_outcomes(kwargs['outcome_path'], c['project_ref'], evaluations)
        return {'status': 'all_archived', 'videos': [], 'dramas': evaluations,
                'delivery_accepted': True}

    def analyze(self, db, storage, cache, did, provider, **kwargs):
        self.events.append('ai')
        if self.fail_ai:
            raise PipelineError('provider_unavailable')
        source = sorted(kwargs['allowed_source_ids'])[0]
        for state in self.cloud.values():
            if state['drama_id'] == did:
                state['tasks']['ai'] = {'status': 'succeeded'}
                state['ai'] = {'drama_id': did, 'source_video_id': source,
                               'title': '剧名', 'tags': ['剧情'], 'description': '简介'}

    async def transcode(self, *args, **kwargs):
        self.events.append('transcode')
        return {'counts': {}}

    def call(self, command='run', *extra, scan=None):
        output = io.StringIO()
        with (patch.object(sys, 'argv', ['pipeline.py', '--data', str(self.root), command, *extra]),
              patch.object(runner, 'BASE', self.root),
              patch.object(runner, 'config', return_value=self.config),
              patch.object(runner, 'Database', return_value=self.db) as database,
              patch.object(runner, 'Storage', return_value=self.storage),
              patch.object(runner, 'Cache'), patch.object(runner, 'backup_legacy'),
              patch.object(runner, 'run_ai', side_effect=self.analyze),
              patch('video_archive.pipeline.workflow.source_session_lock', side_effect=contextlib.nullcontext),
              patch('video_archive.pipeline.workflow.scan_source', side_effect=scan or self.scan),
              patch('video_archive.pipeline.batch.run_batch', side_effect=self.download),
              patch('video_archive.pipeline.batch.drain_transcode', side_effect=self.transcode),
              patch('video_archive.pipeline.ai.build_provider', return_value=object()),
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO())):
            code = runner.main()
            return code, json.loads(output.getvalue()) if output.getvalue() else {}, database.call_count

    def test_default_run_connects_all_stages_and_second_run_skips_media_and_ai(self):
        code, output, _ = self.call()
        self.assertEqual(code, 0)
        self.assertEqual(self.events, ['scan', 'download', 'ai', 'transcode'])
        self.assertEqual(self.downloads, [[11, 12]])
        self.assertEqual(output['workflow_status'], 'complete')
        pool = load_pool(processed_file(self.root, 'testref'), 'testref')
        self.assertEqual(next(iter(pool['dramas'].values()))['status'], 'completed')
        self.events.clear()
        code, output, database_calls = self.call()
        self.assertEqual(code, 0)
        self.assertEqual(self.events, ['scan'])
        self.assertEqual(database_calls, 0)
        self.assertEqual(output['status'], 'nothing_to_process')

    def test_ai_failure_is_not_sealed_and_next_run_resumes_without_redownloading(self):
        self.fail_ai = True
        code, output, _ = self.call()
        self.assertEqual(code, 2)
        self.assertEqual(output['workflow_status'], 'pending')
        self.assertFalse(load_pool(processed_file(self.root, 'testref'), 'testref')['dramas'])
        self.fail_ai = False
        self.events.clear()
        code, output, _ = self.call()
        self.assertEqual(code, 0)
        self.assertEqual(self.downloads, [[11, 12], []])
        self.assertEqual(self.events, ['scan', 'download', 'ai', 'transcode'])

    def test_source_scan_failure_stops_before_database_or_download(self):
        def fail(*args, **kwargs):
            raise PipelineError('metadata_scan_incomplete')
        code, _, database_calls = self.call(scan=fail)
        self.assertEqual(code, 2)
        self.assertEqual(database_calls, 0)
        self.assertFalse(self.downloads)

    def test_skip_scan_requires_existing_complete_proof(self):
        code, _, database_calls = self.call('run', '--skip-scan')
        self.assertEqual(code, 2)
        self.assertEqual(database_calls, 0)
        self.assertFalse(self.events)

    def test_pool_inspection_and_reopen_do_not_require_a_source_snapshot(self):
        code, output, database_calls = self.call('processed')
        self.assertEqual(code, 0)
        self.assertEqual(database_calls, 0)
        self.assertEqual(output['dramas'], {})
        code, _, database_calls = self.call('reopen')
        self.assertEqual(code, 2)
        self.assertEqual(database_calls, 0)


if __name__ == '__main__':
    unittest.main()
