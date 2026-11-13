"""Version bounded enrichment inputs using an audited, already-cached proposal."""
import argparse
from collections import defaultdict
import csv
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import tempfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def has_timezone(value):
    """Accept ISO timestamps with a timezone, including Z on Python 3.10."""
    try:
        normalized = value[:-1] + '+00:00' if value.endswith('Z') else value
        return datetime.fromisoformat(normalized).utcoffset() is not None
    except (AttributeError, TypeError, ValueError):
        return False


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def checked_hash(path, expected):
    raw = path.read_bytes()
    observed = digest(raw)
    normalized = digest(raw.replace(b'\r\n', b'\n'))
    if expected not in (observed, normalized):
        raise ValueError(f'Input checksum mismatch: {path.name}')
    return {'observed_sha256': observed, 'lf_sha256': normalized}


def read_pairs(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != ['tconst', 'nconst']:
            raise ValueError(f'Expected tconst,nconst header: {path}')
        rows = list(reader)
    if any(len(row) != 2 or not re.fullmatch(r'tt[0-9]{7,}', row[0])
           or not re.fullmatch(r'nm[0-9]{7,}', row[1]) for row in rows):
        raise ValueError(f'Invalid pair in {path}')
    pairs = {tuple(row) for row in rows}
    if len(pairs) != len(rows):
        raise ValueError(f'Duplicate pairs in {path}')
    return pairs


def pair_bytes(pairs):
    stream = io.StringIO(newline='')
    writer = csv.writer(stream, lineterminator='\n')
    writer.writerow(['tconst', 'nconst'])
    writer.writerows(sorted(pairs))
    return stream.getvalue().encode('utf-8')


def build(baseline, verified, proposal, evidence, output):
    baseline, verified, output = (p.resolve() for p in (baseline, verified, output))
    sources = (baseline, verified)
    if output.exists() or any(output == p or p in output.parents or output in p.parents for p in sources):
        raise ValueError('Choose a new output separate from all input directories')
    old = load(baseline / 'READY_FOR_REVIEW.json')
    package = load(verified / 'VERIFIED.json')
    proof = load(evidence)
    projection = proof['exact_imdb_credit_projection']
    if package['scope'] != 'full' or proof['applied'] is not False or proof['new_wikimedia_requests'] != 0:
        raise ValueError('Verified full data and an unapplied cache-only proposal required')
    if digest((verified / 'quality/summary.json').read_bytes()) != old['source_prepared_sha256']:
        raise ValueError('Prepared source differs from baseline')
    for name, expected in projection['prepared_source_sha256'].items():
        checked_hash(verified / 'quality' / name, expected)
    checked_hash(verified / 'enrichment/candidates.csv', package['sha256']['enrichment/candidates.csv'])
    checked_hash(proposal, proof['proposal_sha256_lf'])
    used = ['selected-titles.csv', 'top10.csv', 'primary.csv', 'reserves.csv', 'enrichment-input.csv']
    fingerprints = {name: checked_hash(baseline / name, old['files_sha256'][name]) for name in used}
    primary = read_pairs(baseline / 'primary.csv')
    initial_reserves = read_pairs(baseline / 'reserves.csv')
    initial = read_pairs(baseline / 'enrichment-input.csv')
    pool = read_pairs(verified / 'enrichment/candidates.csv')
    if primary & initial_reserves or primary | initial_reserves != initial:
        raise ValueError('Baseline primary/reserve union does not reconcile')
    with proposal.open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream, strict=True))
    extra = {(r['tconst'], r['nconst']) for r in rows}
    if len(extra) != len(rows) or len(extra) != projection['matched_pairs']:
        raise ValueError('Proposal pair counts or uniqueness differ')
    if digest(pair_bytes(extra)) != projection['requested_pairs_sha256_lf']:
        raise ValueError('Proposal pairs differ from the exact IMDb credit projection')
    if extra & initial or not extra <= pool:
        raise ValueError('Supplementary pairs overlap baseline or are absent from the verified pool')
    with (baseline / 'selected-titles.csv').open(encoding='utf-8-sig', newline='') as stream:
        titles = {r['tconst'] for r in csv.DictReader(stream)}
    people = {person for _, person in initial}
    if not {film for film, _ in extra} <= titles or not {person for _, person in extra} <= people:
        raise ValueError('Proposal introduces a new film or person to collect')
    grouped = defaultdict(list)
    for r in rows:
        if (r['biography_available'] != 'True' or int(r['biography_chars']) <= 0
                or r['frozen_cache_sha256'] != proof['snapshot']['sha256']
                or r['imdb_manifest_sha256'] != projection['prepared_source_sha256']['input_manifest.json']
                or not re.fullmatch(r'https://en\.wikipedia\.org/w/index\.php\?oldid=[0-9]+', r['source_url'])
                or not re.fullmatch(r'https://www\.wikidata\.org/wiki/Q[0-9]+', r['wikidata_url'])
                or r['license_url'] != 'https://creativecommons.org/licenses/by-sa/4.0/'
                or not has_timezone(r['retrieved_at_utc'])):
            raise ValueError('Missing cached biography or inconsistent provenance')
        roles = json.loads(r['all_verified_credit_rows'])
        if [int(r['ordering']), r['category']] not in roles:
            raise ValueError('Displayed credit is absent from the verified credit rows')
        grouped[r['tconst']].append(r)
    gain = gain_with_synopsis = 0
    for film, credits in grouped.items():
        before = {int(r['biographies_before']) for r in credits}
        synopsis = {r['synopsis_available'] == 'True' for r in credits}
        if len(credits) > 3 or len(before) != 1 or len(synopsis) != 1:
            raise ValueError('Inconsistent film metadata or more than three supplementary credits')
        before = next(iter(before))
        if before >= 3 or before + len(credits) > 3 or any(int(r['biographies_after']) != before + len(credits) for r in credits):
            raise ValueError('Inconsistent projected biography counts')
        crosses = before + len(credits) == 3
        gain += crosses
        gain_with_synopsis += crosses and next(iter(synopsis))
    if gain != proof['films_with_three_biographies']['gain'] or gain_with_synopsis != proof['films_with_synopsis_and_three']['gain']:
        raise ValueError('Projected coverage gain does not reconcile')
    combined = initial | extra
    reserves = initial_reserves | extra
    if primary & reserves or primary | reserves != combined:
        raise ValueError('Expanded primary/reserve union does not reconcile')
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix='.cached-credit-policy-', dir=output.parent))
    try:
        for name in ['selected-titles.csv', 'top10.csv', 'primary.csv']:
            (stage / name).write_bytes((baseline / name).read_bytes().replace(b'\r\n', b'\n'))
        (stage / 'initial-reserves.csv').write_bytes((baseline / 'reserves.csv').read_bytes().replace(b'\r\n', b'\n'))
        (stage / 'supplementary-provenance.csv').write_bytes(proposal.read_bytes().replace(b'\r\n', b'\n'))
        for name, pairs in [('supplementary.csv', extra), ('reserves.csv', reserves), ('enrichment-input.csv', combined)]:
            (stage / name).write_bytes(pair_bytes(pairs))
        result = {
            'policy_version': 'v2-cached-supplement-13782',
            'decision_owner': 'Nicolas', 'binome_confirmation': 'pending', 'student_validated': False,
            'categories': 'same IMDb principal categories as baseline, including self and archives',
            'primary_policy_unchanged': True, 'initial_reserves_unchanged': True,
            'maximum_supplementary_credits_per_film': 3,
            'supplementary_source_commit': '4ad5567',
            'scope': 'versioned_inputs_and_frozen_snapshot_projection_not_final_spark_results',
            'films': len(titles), 'distinct_people': len(people),
            'primary_pairs': len(primary), 'initial_reserve_pairs': len(initial_reserves),
            'supplementary_pairs': len(extra), 'reserve_pairs': len(reserves), 'input_pairs': len(combined),
            'additional_wikimedia_requests': 0, 'new_lookup_ids': 0,
            'source_prepared_sha256': old['source_prepared_sha256'],
            'source_snapshot': proof['snapshot'], 'raw_cache_independently_rescanned': False,
            'projected_gain_on_same_frozen_snapshot': {'three_biographies': gain, 'synopsis_and_three': gain_with_synopsis},
            'baseline_fingerprints': fingerprints,
            'files_sha256': {p.name: digest(p.read_bytes()) for p in stage.iterdir() if p.is_file()},
        }
        (stage / 'POLICY.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
        os.rename(stage, output)
        return result
    except BaseException:
        shutil.rmtree(stage)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['baseline', 'verified', 'proposal', 'evidence', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.baseline, args.verified, args.proposal, args.evidence, args.output), indent=2))


if __name__ == '__main__':
    main()
