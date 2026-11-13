"""Independent offline audit of the published replay payload; no new live capture."""
import csv,gzip,hashlib,json,re
from pathlib import Path
root=Path(__file__).resolve().parents[1]
p=root/'project/streaming/demo';m=json.loads((p/'manifest.json').read_text())
raw=(p/'demo-events.jsonl.gz').read_bytes();data=gzip.decompress(raw);ref=(p/'demo-reference.csv').read_bytes()
for payload,key in [(raw,'events_gzip_sha256'),(data,'events_jsonl_sha256'),(ref,'reference_sha256')]:
 assert hashlib.sha256(payload).hexdigest()==m[key],key
rows=[json.loads(r) for r in data.decode().splitlines()]
refs={(r['wiki'].strip(),re.sub('[_ ]+',' ',r['title']).strip()) for r in csv.DictReader(ref.decode().splitlines())}
eligible=[r for r in rows if r.get('type')=='edit' and r.get('namespace')==0 and re.fullmatch(r'[a-z0-9-]+\.wikipedia\.org',r.get('server_name','')) and r.get('meta',{}).get('domain')==r['server_name'] and r.get('meta',{}).get('id') is not None and r.get('timestamp') is not None and r.get('title') is not None]
matches=[r for r in eligible if (r['wiki'],re.sub('[_ ]+',' ',r['title']).strip()) in refs]
ids=[r['meta']['id'] for r in rows]
actual={'kafka_records':len(rows),'eligible_wikipedia_edits':len(eligible),'matched_events':len({r['meta']['id'] for r in matches}),'matched_pages':len({(r['wiki'],re.sub('[_ ]+',' ',r['title']).strip()) for r in matches})}
assert len(set(ids))==len(rows)
for key,value in actual.items():assert m[key]==value,(key,value,m[key])
report={'scope':'independent_offline_reconciliation_of_existing_demo_not_new_live_capture','status':'passed','sha256_verified':True,'duplicate_event_ids':0,**actual}
out=root/'results/demo-payload-audit.json';out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
