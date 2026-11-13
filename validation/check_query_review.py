"""Exercise the review on real materialized fixture outputs and corruptions."""
import json
import shutil
import sys
import tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession
from imdb_preparation.review_outputs import run,audit_frame

root=Path(__file__).resolve().parents[1]
fixture=root/'results/cross-pipeline-v1'
spark=(SparkSession.builder.master('local[2]').appName('Query review acceptance checks')
       .config('spark.sql.shuffle.partitions',2).config('spark.ui.enabled','false').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
checks=[]
try:
    result=run(spark,fixture/'analysis',fixture/'prepared',root/'results/query-review-fixture',10)
    assert result['status']=='passed' and result['q2_q3_reconciled'] and result['selection_reconciled']
    assert len(result['questions'])==9
    checks.append('All nine materialized fixture outputs and Q2/Q3 totals accepted')
    config=json.loads((fixture/'analysis/summary.json').read_text())['config']
    ranking=spark.createDataFrame([(2000,'Drama',1,100,1,7.0),(2000,'Comedy',2,200,1,8.0)],
        'startYear long,genre string,genre_rank long,genre_votes long,film_count long,mean_rating double')
    try:
        audit_frame('q2',ranking,config)
    except ValueError as error:
        assert 'rank' in str(error)
    else:
        raise AssertionError('Reversed popularity ranks accepted')
    checks.append('Reversed popularity ordering rejected even with valid row values')
    wrong=spark.createDataFrame([('nm0000001',2,2,5.0)],
       'nconst string,high_rated_films long,other_films long,other_mean_rating double')
    try:
        audit_frame('q4',wrong,config)
    except ValueError:
        pass
    else:
        raise AssertionError('Director with two high-rated films accepted')
    checks.append('A director violating the exactly-one-high-rating condition rejected')
    with tempfile.TemporaryDirectory(prefix='gr04-review-corrupt-') as d:
        copy=Path(d)/'analysis';shutil.copytree(fixture/'analysis',copy)
        summary=json.loads((copy/'summary.json').read_text());summary['questions']['q1']['rows']+=1
        (copy/'summary.json').write_text(json.dumps(summary))
        target=Path(d)/'review'
        try:
            run(spark,copy,fixture/'prepared',target,10)
        except ValueError as error:
            assert 'row count' in str(error)
        else:
            raise AssertionError('Incorrect declared row count accepted')
        assert not target.exists()
    checks.append('Tampered manifest counts rejected without a published success report')
    report={'scope':'controlled_materialized_review_acceptance_not_full_imdb','spark_version':spark.version,
            'status':'passed','checks':checks}
    (root/'results/query-review-acceptance.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
finally:
    spark.stop()
