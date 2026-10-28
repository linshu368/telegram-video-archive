"""Offline audit of an existing topic run; no session access or media downloads."""
import argparse
import json
import platform
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

from core import SECRET_KEYS, atomic_json, now, sha256
from topic_metadata import BASE, Catalog, original_state


def no_secrets(value):
    if isinstance(value, dict):
        assert not (SECRET_KEYS & value.keys()), 'credential_key_in_export'
        for child in value.values():
            no_secrets(child)
    elif isinstance(value, list):
        for child in value:
            no_secrets(child)


def audit(root):
    report = json.loads((root / 'report.json').read_text())
    assert report.get('finished_at'), 'run_not_finished'
    assert report['original_state_unchanged'], 'original_state_was_changed'
    assert original_state() == report['original_state_before'], 'original_state_changed_since_run'
    assert report['media_download_attempts'] == 0
    assert not report['global_history_scanned'] and not report['full_backfill_complete']
    pages = [json.loads(line) for line in (root / 'pages.jsonl').read_text().splitlines()]
    summaries = []
    for run in report['directory_passes']:
        responses = [p for p in pages if p['kind'] == 'directory' and
                     p['pass'] == run['pass'] and p['status'] == 'response_saved']
        advances = [p for p in pages if p['kind'] == 'directory' and
                    p['pass'] == run['pass'] and p['status'] == 'cursor_advanced']
        assert len(responses) == run['pages'] and responses[-1]['topic_ids'] == []
        assert len(advances) == len(responses) - 1
        assert responses[0]['request_cursor'] == [None, 0, 0]
        for index, entry in enumerate(advances):
            assert entry['next_cursor'] == responses[index + 1]['request_cursor']
            assert entry['next_cursor'][2] == responses[index]['topic_ids'][-1]
        ids = [tid for p in responses for tid in p['topic_ids']]
        assert sorted(set(ids)) == run['topic_ids']
        assert len(ids) == len(set(ids)), 'duplicate_directory_ids'
        counts = [p['api_count'] for p in responses]
        assert counts == run['api_counts']
        # Enrich from saved evidence only; do not revise the original result or timestamps.
        run['general_topic_observed'] = 1 in ids
        run['observed_minus_last_api_count'] = len(set(ids)) - counts[-1]
        summaries.append({'pass': run['pass'], 'pages_including_empty': len(responses),
                          'page_sizes': [len(p['topic_ids']) for p in responses],
                          'unique_topics': len(set(ids)), 'api_counts': sorted(set(counts)),
                          'general_topic_observed': 1 in ids,
                          'pinned_ids': sorted({i for p in responses for i in p['pinned_ids']}),
                          'observed_minus_last_api_count': run['observed_minus_last_api_count']})
    if any(s['observed_minus_last_api_count'] for s in summaries):
        note = ('Directory unique-ID count differs from API count; cause not established. '
                'General topic presence is recorded but not assumed to explain the difference.')
        if note not in report['limits']:
            report['limits'].append(note)
    catalog = Catalog(root)
    try:
        assert catalog.db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        report['offline_export_refreshed_at'] = now()
        catalog.export(report)
    finally:
        catalog.db.close()
    records = [json.loads(line) for line in (root / 'messages.jsonl').read_text().splitlines()]
    topics = json.loads((root / 'topics.json').read_text())
    dramas = json.loads((root / 'dramas.json').read_text())
    selected = next(r for r in records if r['message_id'] == 6559)
    assert selected['topic_id'] == 6557
    assert selected['reply']['forum_topic'] and selected['reply']['reply_to_msg_id'] == 6557
    assert selected['topic_title'] == report['selected_video_evidence']['topic_title']
    assert selected['text'] == '第2集' and selected['original_filename'] == '2.1.mp4'
    assert selected['title'] == '', 'caption_title_was_overwritten'
    file = Path(selected['download']['local_path'])
    assert file.stat().st_size == selected['expected_size']
    assert sha256(file) == selected['download']['sha256']
    assert not list(file.parent.glob('*.part')), 'probe_partial_file_remains'
    reply_pages = [p for p in pages if p['kind'] == 'replies' and p['status'] == 'response_saved']
    assert reply_pages[-1]['message_ids'] == []
    reply_ids = {mid for p in reply_pages for mid in p['message_ids']}
    assert reply_ids == set(report['selected_topic_replies']['message_ids'])
    assert 6559 in reply_ids
    assert all(r['topic_id'] == 6557 for r in records), 'unexpected_topic_membership'
    assert len(topics) == len({(t['source_id'], t['topic_id']) for t in topics})
    assert sum(d['observed_video_count'] for d in dramas) == report['observed_videos']
    assert all(d['accessible_video_total'] is None for d in dramas)
    for path in root.iterdir():
        if path.suffix == '.json':
            no_secrets(json.loads(path.read_text()))
        elif path.suffix == '.jsonl':
            for line in path.read_text().splitlines():
                no_secrets(json.loads(line))
    db = BASE / 'archive/state.sqlite3'
    connection = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
    try:
        integrity = connection.execute('PRAGMA integrity_check').fetchone()[0]
        main_counts = {t: connection.execute('SELECT count(*) FROM ' + t).fetchone()[0]
                       for t in ('messages', 'videos', 'pages', 'topics')}
        assert integrity == 'ok' and not any(main_counts.values())
    finally:
        connection.close()
    assert original_state() == report['original_state_before']
    return {'directory_passes': summaries, 'directory_status': report['directory_status'],
            'two_pass_sets_and_titles_equal': report['directory_two_pass_sets_and_titles_equal'],
            'topic_status_counts': dict(Counter(t['status'] for t in topics)),
            'selected_video_evidence': report['selected_video_evidence'],
            'observed_messages': len(records), 'observed_videos': report['observed_videos'],
            'video_ids': [r['message_id'] for r in records if r['video_evidence']],
            'verified_existing_file_ids': [r['message_id'] for r in records if r.get('download')],
            'global_history_scanned': False, 'global_video_total': None,
            'main_archive_counts': main_counts, 'main_archive_integrity': integrity,
            'main_archive_result': json.loads((BASE / 'archive/report.json').read_text())['result'],
            'original_state_unchanged': True, 'credential_key_audit_passed': True,
            'output': str(root)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    tests = subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', str(BASE / 'tests'), '-v'],
                           cwd=BASE, capture_output=True, text=True)
    log = BASE / 'archive/topics-offline-tests.txt'
    log.write_text(tests.stdout + tests.stderr)
    pip = subprocess.run([sys.executable, '-m', 'pip', 'check'], capture_output=True, text=True)
    result = {'generated_at': now(), 'python': platform.python_version(),
              'test_exit_code': tests.returncode, 'test_log': str(log),
              'pip_check_exit_code': pip.returncode, 'pip_check_output': pip.stdout.strip()}
    try:
        result.update(audit(root))
        result['passed'] = tests.returncode == 0 and pip.returncode == 0
    except Exception as exc:
        result.update(passed=False, failure_type=type(exc).__name__)
    atomic_json(root / 'acceptance.json', result)
    print(json.dumps({k: result[k] for k in ['passed', 'test_exit_code', 'pip_check_exit_code']}, indent=2))
    print('acceptance=' + str(root / 'acceptance.json'))
    return 0 if result['passed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())