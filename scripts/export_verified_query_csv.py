"""Export complete materialized query results; preserve arrays as JSON CSV cells."""
import argparse
import hashlib
import json
from pathlib import Path
import duckdb

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--verified-root', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
a = parser.parse_args()
source = a.verified_root.resolve()
output = a.output.resolve()
if output.exists() or output == source or source in output.parents or output in source.parents:
    raise ValueError('Choose a new output directory separate from the verified input')
marker = json.loads((source / 'VERIFIED.json').read_text(encoding="utf-8"))
summary = json.loads((source / 'analysis/summary.json').read_text(encoding="utf-8"))
assert marker['scope'] == summary['scope'] == 'full'
for name, expected in marker['sha256'].items():
    path = (source / name).resolve()
    assert source in path.parents
    if not path.exists() and path.name.startswith('.') and path.name.endswith('.crc'):
        continue  # upload-artifact omits optional Hadoop checksum sidecars.
    assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, name
output.mkdir(parents=True)
db = duckdb.connect(config={'threads': 2, 'memory_limit': '1GB'})
report = {'scope': 'complete_materialized_query_csv_exports', 'source_scope': 'full',
          'student_validated': summary['config']['student_validated'],
          'duckdb': duckdb.__version__, 'queries': {}}
for name, evidence in summary['questions'].items():
    table = db.read_parquet(str(source / 'analysis' / name / '*.parquet'))
    fields = []
    arrays = []
    for column, dtype in zip(table.columns, table.dtypes):
        quoted = '"' + column.replace('"', '""') + '"'
        if str(dtype).endswith('[]') or str(dtype).startswith(('STRUCT(', 'MAP(')):
            fields.append(f'to_json({quoted}) AS {quoted}')
            arrays.append(column)
        else:
            fields.append(quoted)
    destination = output / (name + '.csv')
    table.project(', '.join(fields)).write_csv(str(destination), header=True)
    count = db.read_csv(str(destination), header=True, all_varchar=True).count('*').fetchone()[0]
    assert count == evidence['rows'], name
    report['queries'][name] = {'rows': count, 'json_columns': arrays,
                               'sha256': hashlib.sha256(destination.read_bytes()).hexdigest()}
(output / 'COMPLETE_CSV_EXPORT.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
print(json.dumps(report, indent=2))
