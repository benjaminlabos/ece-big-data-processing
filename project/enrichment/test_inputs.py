import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from collect import cached_record_is_valid, candidate_ids, get_json, write_record
from inputs import CANDIDATE_COLUMNS, read_ids


class EnrichmentInputsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.credits = self.root / "credits.csv"

    def test_rejects_inverted_header_ids_and_short_ids(self):
        for source in (
            "nconst,tconst\nnm0000001,tt0000001\n",
            "tconst,nconst\nnm0000001,tt0000001\n",
            "tconst,nconst\ntt1,nm0000001\n",
            "tconst,nconst\ntt0000001,nm0000001,extra\n",
            'tconst,nconst\n"tt0000001,nm0000001\n',
        ):
            with self.subTest(source=source):
                self.credits.write_text(source, encoding="utf-8")
                with self.assertRaises(ValueError):
                    list(read_ids(self.credits, CANDIDATE_COLUMNS))

    def test_collects_repeated_person_once_and_keeps_film_with_no_candidates(self):
        self.credits.write_text(
            "tconst,nconst\ntt0000001,nm0000001\ntt0000002,nm0000001\n",
            encoding="utf-8",
        )
        titles = self.root / "titles.csv"
        titles.write_text("tconst\ntt0000001\ntt0000002\ntt0000003\n", encoding="utf-8")
        self.assertEqual(
            candidate_ids(self.credits, titles),
            ["tt0000001", "nm0000001", "tt0000002", "tt0000003"],
        )
        titles.write_text("tconst\ntt0000002\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "absent"):
            candidate_ids(self.credits, titles)

    def test_cache_record_is_written_as_one_json_file(self):
        record = {"imdb_id": "nm0000001", "missing_reason": "no_wikidata_item"}
        write_record(self.root, record)
        files = list(self.root.iterdir())
        self.assertEqual([path.name for path in files], ["nm0000001.json"])
        self.assertIn('"no_wikidata_item"', files[0].read_text(encoding="utf-8"))

    def test_retries_temporary_response_and_respects_retry_after(self):
        limited = Mock(status_code=429, headers={"Retry-After": "9"})
        ok = Mock(status_code=200, **{"json.return_value": {"query": "ok"}})
        session = Mock(**{"get.side_effect": [requests.Timeout(), limited, ok]})
        with patch("collect.time.sleep") as sleep:
            self.assertEqual(get_json(session, "https://example.org", {}), {"query": "ok"})
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 1, 1, 9, 1])

    def test_reports_retry_delay_when_it_cannot_retry_in_this_run(self):
        limited = Mock(status_code=429, headers={"Retry-After": "180"})
        with patch("collect.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "Retry after at least 180 seconds"):
                get_json(Mock(**{"get.return_value": limited}), "https://example.org", {})

    def test_corrupt_cache_is_pending_until_a_complete_record_replaces_it(self):
        imdb_id = "nm0000001"
        path = self.root / f"{imdb_id}.json"
        path.write_text("{", encoding="utf-8")
        with patch("collect.sys.stderr"):
            self.assertFalse(cached_record_is_valid(self.root, imdb_id))
        write_record(self.root, {
            "imdb_id": imdb_id,
            "retrieved_at_utc": "2026-11-11T00:00:00+00:00",
            "missing_reason": "no_wikidata_item",
        })
        self.assertTrue(cached_record_is_valid(self.root, imdb_id))


if __name__ == "__main__":
    unittest.main()
