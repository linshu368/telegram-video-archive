"""LLM text structuring with a fake completion; no network, no services."""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_archive.pipeline.common import PipelineError, identity
from video_archive.pipeline.text_structuring import (build_bundle, structure_cache_file, structure_drama,
                                                     structure_jobs, validate_structure)

CFG = {'model': 'm', 'base_url': 'https://example.test', 'api_key': 'k', 'request_timeout': 10}
SOURCE = -100123


def jobs(topic=20, texts=('第1集 #甜宠', '第2集', '')):
    did = identity('topic', SOURCE, topic)
    result = []
    for index, body in enumerate(texts):
        mid = 100 + index
        result.append(({'id': did, 'title': f'话题{topic}', 'source_id': str(SOURCE), 'topic_id': str(topic)},
                       {'id': identity('message', SOURCE, mid), 'drama_id': did,
                        'original_title': 'regex', 'original_tags': ['regex'],
                        'original_description': 'regex', 'episode': 'regex'},
                       {'message_id': mid, 'text': body}, {'status': 'pending'}))
    return result


def good(bundle, **drama):
    episodes = iter([1, 2, None])
    return {'drama': {'title': '剧名', 'description': '简介', 'tags': ['甜宠'], **drama},
            'videos': [{'message_id': m['message_id'], 'episode': next(episodes)}
                       for m in bundle['messages']]}


class Completion:
    def __init__(self, *outputs):
        self.outputs, self.calls = list(outputs), 0

    def __call__(self, bundle, cfg, check):
        self.calls += 1
        output = self.outputs.pop(0) if self.outputs else good(bundle)
        return output(bundle) if callable(output) else output


class ValidateTests(unittest.TestCase):
    def setUp(self):
        self.bundle = build_bundle('话题', jobs())

    def test_valid_output_is_normalized(self):
        value = good(self.bundle, title=' 剧名 ', description='  ', tags=['a', 'a', ' '])
        result = validate_structure(self.bundle, value)
        self.assertEqual((result['title'], result['description'], result['tags']), ('剧名', None, ['a']))
        self.assertEqual(result['episodes'], {100: 1, 101: 2, 102: None})

    def test_rejects_bad_shapes(self):
        def mutated(fn):
            value = good(self.bundle)
            fn(value)
            return value
        cases = [
            [], {'drama': {}, 'videos': []},
            mutated(lambda v: v['drama'].pop('tags')),
            mutated(lambda v: v['drama'].update(tags='x')),
            mutated(lambda v: v['drama'].update(title=1)),
            mutated(lambda v: v['videos'].pop()),
            mutated(lambda v: v['videos'].append({'message_id': 999, 'episode': None})),
            mutated(lambda v: v['videos'][0].update(episode='1')),
            mutated(lambda v: v['videos'][0].update(episode=0)),
            mutated(lambda v: v['videos'][0].update(episode=True)),
            mutated(lambda v: v['videos'][1].update(episode=1)),
            mutated(lambda v: v['videos'][0].update(message_id='100')),
            mutated(lambda v: v['videos'][0].update(extra=1)),
        ]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(PipelineError) as ctx:
                validate_structure(self.bundle, value)
            self.assertEqual(str(ctx.exception), 'invalid_text_structure')

    def test_one_retry_on_invalid_then_gives_up(self):
        flaky = Completion({'bad': 1}, good)
        self.assertEqual(structure_drama(self.bundle, CFG, complete=flaky)['title'], '剧名')
        self.assertEqual(flaky.calls, 2)
        broken = Completion({'bad': 1}, {'bad': 2}, good)
        with self.assertRaises(PipelineError):
            structure_drama(self.bundle, CFG, complete=broken)
        self.assertEqual(broken.calls, 2)

    def test_transport_error_is_not_retried(self):
        def boom(bundle, cfg, check):
            boom.calls += 1
            raise OSError('down')
        boom.calls = 0
        with self.assertRaises(OSError):
            structure_drama(self.bundle, CFG, complete=boom)
        self.assertEqual(boom.calls, 1)


class StructureJobsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cache = structure_cache_file(self.tmp.name, 'a' * 20)

    def run_jobs(self, rows, complete, **kwargs):
        return structure_jobs(rows, cfg=CFG, cache_path=self.cache, complete=complete, **kwargs)

    def test_drama_fields_shared_and_episode_per_video(self):
        rows = jobs()
        report = self.run_jobs(rows, Completion())
        self.assertEqual(report[0]['result'], 'structured')
        videos = [row[1] for row in rows]
        self.assertEqual({v['original_title'] for v in videos}, {'剧名'})
        self.assertEqual({v['original_description'] for v in videos}, {'简介'})
        self.assertEqual({tuple(v['original_tags']) for v in videos}, {('甜宠',)})
        self.assertEqual([v['episode'] for v in videos], ['1', '2', None])

    def test_cache_prevents_second_call_and_survives_json_roundtrip(self):
        first = Completion()
        self.run_jobs(jobs(), first)
        again, rows = Completion(), jobs()
        report = self.run_jobs(rows, again)
        self.assertEqual((first.calls, again.calls, report[0]['result']), (1, 0, 'cached'))
        self.assertEqual([row[1]['episode'] for row in rows], ['1', '2', None])
        json.loads(self.cache.read_text())

    def test_changed_text_misses_cache(self):
        self.run_jobs(jobs(), Completion())
        again = Completion()
        self.run_jobs(jobs(texts=('第1集', '第2集', '新')), again)
        self.assertEqual(again.calls, 1)

    def test_failure_keeps_regex_fields_and_other_dramas_still_run(self):
        bad, ok = jobs(topic=20), jobs(topic=21)
        before = copy.deepcopy([row[1] for row in bad])
        failing = Completion({'bad': 1}, {'bad': 2})
        report = self.run_jobs(bad + ok, failing)
        self.assertEqual([r['result'] for r in report], ['fallback_regex', 'structured'])
        self.assertEqual(report[0]['error'], 'invalid_text_structure')
        self.assertEqual([row[1] for row in bad], before)
        self.assertEqual(ok[0][1]['original_title'], '剧名')

    def test_network_failure_falls_back(self):
        def boom(bundle, cfg, check):
            raise OSError('down')
        rows = jobs()
        report = self.run_jobs(rows, boom)
        self.assertEqual(report[0]['result'], 'fallback_regex')
        self.assertEqual(rows[0][1]['original_title'], 'regex')
        self.assertFalse(self.cache.exists())

    def test_fully_imported_drama_is_skipped(self):
        rows, complete = jobs(), Completion()
        report = self.run_jobs(rows, complete, skip_message_ids={100, 101, 102})
        self.assertEqual((report[0]['result'], complete.calls), ('skipped_already_imported', 0))
        self.assertEqual(rows[0][1]['original_title'], 'regex')
        partial = self.run_jobs(jobs(), Completion(), skip_message_ids={100})
        self.assertEqual(partial[0]['result'], 'structured')

    def test_oversized_bundle_falls_back_without_call(self):
        complete = Completion()
        report = self.run_jobs(jobs(texts=('x' * 70_000,)), complete)
        self.assertEqual((report[0]['error'], complete.calls), ('text_bundle_too_large', 0))

    def test_hint_and_empty_text_reach_the_model(self):
        seen = []

        def spy(bundle, cfg, check):
            seen.append(bundle)
            return good(bundle)
        self.run_jobs(jobs(), spy)
        self.assertEqual(seen[0]['drama_hint'], '话题20')
        self.assertEqual([m['text'] for m in seen[0]['messages']], ['第1集 #甜宠', '第2集', ''])


if __name__ == '__main__':
    unittest.main()
