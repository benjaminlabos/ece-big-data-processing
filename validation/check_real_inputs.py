"""Validate the existing provisional inputs with Benjamin's actual CSV reader."""
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root/'project/enrichment'))
from inputs import read_ids,CANDIDATE_COLUMNS,TITLE_COLUMNS
from collect import candidate_ids
v=root/'validation';suffix='full-provisional-2026-11-11.csv'
def path(kind):return v/f'{kind}-{suffix}'
def rows(kind,cols):
 r=list(read_ids(path(kind),cols));assert len(r)==len(set(r)),kind
 return set(r)
titles=rows('selected-titles',TITLE_COLUMNS)
pairs=rows('enrichment-input',CANDIDATE_COLUMNS)
primary=rows('enrichment-primary',CANDIDATE_COLUMNS)
reserve=rows('enrichment-reserves',CANDIDATE_COLUMNS)
assert not primary&reserve and primary|reserve==pairs
assert {t for t,n in pairs}<={t[0] for t in titles}
requested=candidate_ids(path('enrichment-input'),path('selected-titles'))
expected={t[0] for t in titles}|{n for t,n in pairs}
assert set(requested)==expected and len(requested)==len(expected)
top=list(csv.DictReader(path('top10').open(encoding='utf-8',newline='')))
assert {r['tconst'] for r in top}=={t[0] for t in titles}
groups=defaultdict(list)
for r in top:
 assert 1980<=int(r['startYear'])<=2025
 assert int(r['runtimeMinutes'])>=40 and int(r['numVotes'])>=1000
 groups[int(r['startYear']),r['genre']].append(r)
for key,rr in groups.items():
 rr.sort(key=lambda r:int(r['genre_year_rank']))
 assert [int(r['genre_year_rank']) for r in rr]==list(range(1,len(rr)+1))
 assert len(rr)<=10 and len({r['tconst'] for r in rr})==len(rr)
 assert rr==sorted(rr,key=lambda r:(-float(r['averageRating']),-int(r['numVotes']),r['tconst']))
coverage=list(csv.DictReader(path('enrichment-coverage').open(encoding='utf-8',newline='')))
assert {r['tconst'] for r in coverage}=={t[0] for t in titles}
pc,rc=Counter(t for t,n in primary),Counter(t for t,n in reserve)
for r in coverage:
 assert int(r['primary_people'])==pc[r['tconst']]
 assert int(r['reserve_people'])==rc[r['tconst']]
short=[r['tconst'] for r in coverage if int(r['candidate_people'])<3]
report={'scope':'real_provisional_csv_contract_audit_not_completed_full_batch',
 'verified_at_utc':datetime.now(timezone.utc).isoformat(),'status':'passed',
 'films':len(titles),'input_pairs':len(pairs),'priority_pairs':len(primary),
 'reserve_pairs':len(reserve),'unique_people':len({n for t,n in pairs}),
 'collector_expected_ids':len(requested),'ranking_places':len(top),'year_genre_groups':len(groups),
 'films_below_three_candidates':sorted(short),'student_validated':False,
 'checks':['Actual Benjamin CSV and collector readers accepted all inputs','No duplicate or inverted IDs','Disjoint priority/reserve union exact','Top membership and tie breaks checked','Provisional duration/votes/year bounds checked','Coverage and candidate counts reconciled'],
 'sha256':{kind:hashlib.sha256(path(kind).read_bytes()).hexdigest() for kind in
 ['selected-titles','enrichment-input','enrichment-primary','enrichment-reserves','top10','enrichment-coverage']}}
out=root/'results/real-input-contract-audit.json';out.parent.mkdir(exist_ok=True)
out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
