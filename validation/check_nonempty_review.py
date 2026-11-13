"""Run existing nonempty query fixtures through the new output contracts."""
import importlib.util
import json
import sys
from pathlib import Path
sys.path.insert(0,'/job')
from pyspark.sql import SparkSession
from imdb_preparation.review_outputs import audit_frame
root=Path('/job')
spec=importlib.util.spec_from_file_location('exploration_fixture_checks',root/'tests/test_exploration.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
original=module.analyses
checked=set()
def with_output_review(tables,config,languages=None):
    frames=original(tables,config,languages)
    if languages is not None:
        for name,frame in frames.items():
            audit_frame(name,frame,config)
            checked.add(name)
    return frames
module.analyses=with_output_review
spark=(SparkSession.builder.master('local[2]').appName('Nonempty query output contracts')
       .config('spark.sql.shuffle.partitions',2).config('spark.ui.enabled','false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    module.test_all_explorations_and_top10_on_controlled_data(spark)
    assert checked=={f'q{i}' for i in range(1,9)}|{'top10_per_genre_year'}
    report={'scope':'controlled_nonempty_query_fixtures_not_full_imdb','spark_version':spark.version,
            'status':'passed','query_outputs_checked':sorted(checked),
            'existing_exploration_assertions':'passed'}
    (root/'results/nonempty-query-review.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
finally:
    spark.stop()
