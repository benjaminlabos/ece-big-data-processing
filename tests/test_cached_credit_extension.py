"""Controlled input-policy checks; no network and no human-authored text fixtures."""
import csv
import io
import json

import pytest
from scripts.expand_cached_credits import build, digest, pair_bytes


def fixture(root, people=(3,), crlf=False, duplicate=False):
    baseline, verified = root / 'baseline', root / 'verified'
    baseline.mkdir(); (verified / 'quality').mkdir(parents=True); (verified / 'enrichment').mkdir()
    title = lambda n: f'tt{n:07d}'
    person = lambda n: f'nm{n:07d}'
    primary = {(title(1), person(1)), (title(1), person(2))} | {(title(2), person(n)) for n in (3, 4, 5)}
    reserves = {(title(2), person(6))}
    extra = {(title(1), person(n)) for n in people}
    payloads = {'primary.csv': pair_bytes(primary), 'reserves.csv': pair_bytes(reserves),
                'enrichment-input.csv': pair_bytes(primary | reserves),
                'selected-titles.csv': b'tconst\ntt0000001\ntt0000002\n',
                'top10.csv': b'startYear,genre,genre_year_rank,tconst,primaryTitle,runtimeMinutes,averageRating,numVotes\n2000,Drama,1,tt0000001,TEST,100,7,1000\n2000,Drama,2,tt0000002,TEST,100,7,1000\n'}
    for name, data in payloads.items():
        (baseline / name).write_bytes(data.replace(b'\n', b'\r\n') if crlf else data)
    summary = b'{"scope":"full"}'
    manifest = b'{"scope":"full","test_fixture":true}'
    (verified / 'quality/summary.json').write_bytes(summary)
    (verified / 'quality/input_manifest.json').write_bytes(manifest)
    pool = pair_bytes(primary | reserves | extra)
    (verified / 'enrichment/candidates.csv').write_bytes(pool)
    (verified / 'VERIFIED.json').write_text(json.dumps({'scope': 'full', 'sha256': {'enrichment/candidates.csv': digest(pool)}}))
    (baseline / 'READY_FOR_REVIEW.json').write_text(json.dumps({'source_prepared_sha256': digest(summary), 'files_sha256': {k: digest(v) for k, v in payloads.items()}}))
    records = [{'tconst': t, 'nconst': n, 'biography_available': 'True', 'biography_chars': '20',
                'frozen_cache_sha256': 'f' * 64, 'imdb_manifest_sha256': digest(manifest),
                'source_url': 'https://en.wikipedia.org/w/index.php?oldid=1',
                'wikidata_url': 'https://www.wikidata.org/wiki/Q1',
                'license_url': 'https://creativecommons.org/licenses/by-sa/4.0/',
                'retrieved_at_utc': '2026-11-12T00:00:00Z', 'ordering': '10', 'category': 'actor',
                'all_verified_credit_rows': '[[10,"actor"]]', 'biographies_before': '2',
                'biographies_after': str(2 + len(extra)), 'synopsis_available': 'True'} for t, n in sorted(extra)]
    if duplicate:
        records.append(records[0])
    stream = io.StringIO(newline=''); writer = csv.DictWriter(stream, fieldnames=list(records[0]), lineterminator='\n')
    writer.writeheader(); writer.writerows(records)
    data = stream.getvalue().encode(); proposal = root / 'proposal.csv'
    proposal.write_bytes(data.replace(b'\n', b'\r\n') if crlf else data)
    proof = {'applied': False, 'new_wikimedia_requests': 0, 'proposal_sha256_lf': digest(data),
             'snapshot': {'sha256': 'f' * 64}, 'exact_imdb_credit_projection': {
                 'matched_pairs': len(extra), 'requested_pairs_sha256_lf': digest(pair_bytes(extra)),
                 'prepared_source_sha256': {'summary.json': digest(summary), 'input_manifest.json': digest(manifest)}},
             'films_with_three_biographies': {'gain': 1}, 'films_with_synopsis_and_three': {'gain': 1}}
    evidence = root / 'proof.json'; evidence.write_text(json.dumps(proof))
    return baseline, verified, proposal, evidence, root / 'output'


def test_preserves_inputs_and_adds_no_lookup_ids(tmp_path):
    args = fixture(tmp_path)
    result = build(*args)
    assert result['input_pairs'] == 7 and result['distinct_people'] == 6
    assert result['supplementary_pairs'] == 1 and result['new_lookup_ids'] == 0
    assert result['projected_gain_on_same_frozen_snapshot'] == {'three_biographies': 1, 'synopsis_and_three': 1}
    assert (args[-1] / 'primary.csv').read_bytes() == (args[0] / 'primary.csv').read_bytes()
    assert not result['student_validated'] and result['binome_confirmation'] == 'pending'
    with pytest.raises(ValueError, match='new output'):
        build(*args)


def test_accepts_windows_line_endings_without_rewriting_sources(tmp_path):
    args = fixture(tmp_path, crlf=True)
    original = args[2].read_bytes()
    result = build(*args)
    assert result['input_pairs'] == 7
    assert args[2].read_bytes() == original
    assert result['baseline_fingerprints']['primary.csv']['observed_sha256'] != result['baseline_fingerprints']['primary.csv']['lf_sha256']


@pytest.mark.parametrize('people,message', [((99,), 'new film or person'), ((1,), 'overlap'), ((3, 4, 5, 6), 'more than three')])
def test_rejects_new_ids_overlap_and_unbounded_expansion(tmp_path, people, message):
    args = fixture(tmp_path, people=people)
    with pytest.raises(ValueError, match=message):
        build(*args)
    assert not args[-1].exists()


def test_rejects_duplicate_supplementary_pairs(tmp_path):
    args = fixture(tmp_path, duplicate=True)
    with pytest.raises(ValueError, match='uniqueness'):
        build(*args)
    assert not args[-1].exists()


def test_rejects_altered_proposal_bytes(tmp_path):
    args = fixture(tmp_path)
    args[2].write_bytes(args[2].read_bytes() + b'corruption\n')
    with pytest.raises(ValueError, match='checksum'):
        build(*args)
    assert not args[-1].exists()
