"""Portable CLI paths retain the internal configuration and link checks."""
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.maintenance import backfill_video_dimensions as maintenance
from video_archive.paths import BASE, cli_path
from video_archive.pipeline import runner
from video_archive.telegram import archive
from test_drama_pipeline_cli import source_inventory
from test_metadata_scan import MetadataClient, dependencies, video


@contextmanager
def working_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


class CliPathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.previous_umask = os.umask(0o077)

    def tearDown(self):
        os.umask(self.previous_umask)
        self.temp.cleanup()

    def private_config(self):
        folder = self.root / '.private'
        folder.mkdir(mode=0o700)
        path = folder / 'settings.json'
        ref = 'a' * 20
        path.write_text(json.dumps({
            'project_ref': ref, 'region': 'test', 'plan': 'pro',
            'database_dsn': f'host=db.{ref}.supabase.co dbname=postgres user=test sslmode=require',
            'storage_key': 'test-only', 'global_file_limit': 1_000_000,
            'bucket_file_limit': 1_000_000, 'sample_budget_confirmed': True,
            'dashboard_verified_at': datetime.now(timezone.utc).isoformat(),
            'sample_budget_note': 'offline path test',
        }))
        path.chmod(0o600)
        return path

    def test_normalization_preserves_link_identity_and_expands_user_paths(self):
        target = self.root / 'target.json'
        target.write_text('{}')
        link = self.root / 'link.json'
        link.symlink_to(target)
        with working_directory(self.root):
            normalized = cli_path('link.json')
            self.assertEqual(normalized, link)
            self.assertTrue(normalized.is_absolute())
            self.assertTrue(normalized.is_symlink())
            self.assertNotEqual(normalized, target)
            self.assertEqual(cli_path('~/settings.json'), Path.home() / 'settings.json')

    def test_relative_plan_data_topics_and_report_are_offline(self):
        source_inventory(self.root / 'archive')
        output = io.StringIO()
        args = ['pipeline.py', '--data', 'archive', '--topics', 'archive/old-topics.json',
                'plan', '--report', 'archive/selection.json']
        with (working_directory(self.root), patch.object(sys, 'argv', args),
              patch.object(runner, 'Database') as database,
              patch.object(runner, 'Storage') as storage, redirect_stdout(output)):
            self.assertEqual(runner.main(), 0)
        database.assert_not_called()
        storage.assert_not_called()
        report = json.loads((self.root / 'archive/selection.json').read_text())
        self.assertEqual(report['dramas'][0]['expected_message_ids'], [11, 12])
        self.assertEqual(report['eligible_before_cloud_check'], 2)
        self.assertFalse(report['remote_access'])
        self.assertEqual(json.loads(output.getvalue())['report'],
                         str(self.root / 'archive/selection.json'))

    def test_relative_plan_report_does_not_follow_an_existing_link(self):
        source_inventory(self.root / 'archive')
        target = self.root / 'untouched.json'
        target.write_text('{"preserve": true}')
        report = self.root / 'archive/selection.json'
        report.symlink_to(target)
        args = ['pipeline.py', '--data', 'archive', 'plan', '--report', 'archive/selection.json']
        with (working_directory(self.root), patch.object(sys, 'argv', args),
              redirect_stdout(io.StringIO())):
            self.assertEqual(runner.main(), 0)
        self.assertEqual(target.read_text(), '{"preserve": true}')
        self.assertFalse(report.is_symlink())
        self.assertEqual(json.loads(report.read_text())['dramas'][0]['decision'], 'accept')

    def test_relative_metadata_scan_writes_to_requested_directory_without_media(self):
        private = self.root / '.private'
        private.mkdir(mode=0o700)
        client = MetadataClient([video(1)])
        with ExitStack() as stack:
            stack.enter_context(working_directory(self.root))
            forbidden, _ = dependencies(stack, client)
            stack.enter_context(patch.object(archive, 'PRIVATE', private))
            stack.enter_context(patch.object(sys, 'argv', ['archive.py', '--data', 'archive', 'scan']))
            self.assertEqual(archive.main(), 0)
            for mock in forbidden.values():
                mock.assert_not_called()
        report = json.loads((self.root / 'archive/report.json').read_text())
        self.assertTrue(report['last_operation']['metadata_scan_complete'])
        self.assertEqual(report['video_states_mutually_exclusive']['pending'], 1)
        self.assertEqual(list((self.root / 'archive/videos').iterdir()), [])

    def test_relative_config_is_normalized_before_original_validation(self):
        path = self.private_config()
        args = ['pipeline.py', '--config', '.private/settings.json', 'check']
        with (working_directory(self.root), patch.object(sys, 'argv', args),
              patch.object(runner, 'config', wraps=runner.config) as load_config,
              patch.object(runner, 'Database') as database,
              patch.object(runner, 'Storage') as storage, redirect_stdout(io.StringIO())):
            database.return_value.verify_worker.return_value = {}
            storage.return_value.bucket.return_value = {}
            storage.return_value.cover_bucket.return_value = {}
            self.assertEqual(runner.main(), 0)
        load_config.assert_called_once_with(path)

    def test_relative_config_link_is_still_rejected_before_external_access(self):
        target = self.private_config()
        (target.parent / 'link.json').symlink_to(target)
        error = io.StringIO()
        args = ['pipeline.py', '--config', '.private/link.json', 'check']
        with (working_directory(self.root), patch.object(sys, 'argv', args),
              patch.object(runner, 'Database') as database,
              patch.object(runner, 'Storage') as storage, redirect_stderr(error)):
            self.assertEqual(runner.main(), 2)
        database.assert_not_called()
        storage.assert_not_called()
        self.assertIn('private_pipeline_config_missing', error.getvalue())

    def test_default_data_directory_stays_at_project_root(self):
        with working_directory(self.root):
            self.assertEqual(archive.parser().parse_args(['scan']).data, BASE / 'archive')

    def test_maintenance_normalizes_both_paths_before_owner_lookup(self):
        catalog = self.root / 'archive/index.csv'
        catalog.parent.mkdir()
        catalog.write_text('source_id,message_id,width,height\n')
        args = ['backfill_video_dimensions.py', '--config', '.private/owner.json',
                '--catalog', 'archive/index.csv']
        sentinel = RuntimeError('stop before database')
        with (working_directory(self.root), patch.object(sys, 'argv', args),
              patch.object(maintenance, 'catalog', return_value={}) as read_catalog,
              patch.object(maintenance, 'load_owner', side_effect=sentinel) as load_owner,
              patch.object(maintenance.psycopg, 'connect') as connect):
            with self.assertRaisesRegex(RuntimeError, 'stop before database'):
                maintenance.main()
        read_catalog.assert_called_once_with(catalog)
        load_owner.assert_called_once_with(self.root / '.private/owner.json')
        connect.assert_not_called()

    def test_maintenance_relative_catalog_link_is_still_rejected(self):
        target = self.root / 'real.csv'
        target.write_text('source_id,message_id,width,height\n')
        (self.root / 'index.csv').symlink_to(target)
        args = ['backfill_video_dimensions.py', '--catalog', 'index.csv']
        with (working_directory(self.root), patch.object(sys, 'argv', args),
              patch.object(maintenance, 'load_owner') as load_owner,
              patch.object(maintenance.psycopg, 'connect') as connect,
              redirect_stderr(io.StringIO())):
            with self.assertRaises(SystemExit):
                maintenance.main()
        load_owner.assert_not_called()
        connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
