"""Offline cross-branch contract audit; every input and text is synthetic."""
import csv
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT/'project/enrichment'), str(ROOT/'05.structured-streaming/lab_docker_wikistreams/jobs')]
from pyspark.sql import SparkSession
from imdb_preparation.fixture import write_fixture
from imdb_preparation.prepare import prepare
from imdb_preparation.explore import run as explore
from imdb_preparation.exports import export
from enrich import write_results
from inputs import read_ids, CANDIDATE_COLUMNS
from project_stream import reference_table


def main():
    out = ROOT/'results/cross-pipeline-v1'
    out.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.master('local[2]').appName('gr04-cross-branch-audit')
             .config('spark.sql.shuffle.partitions','2').config('spark.ui.enabled','false')
             .config('spark.sql.session.timeZone','UTC').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    report = {'scope':'synthetic_cross_branch_contract_test_not_real_imdb',
              'started_at_utc':datetime.now(timezone.utc).isoformat(), 'spark_version':spark.version,
              'assertions':[]}
    try:
        raw, prepared, analysis, exports = (out/n for n in ('raw','prepared','analysis','exports'))
        write_fixture(raw)
        prepare(spark, raw, prepared, 2026, input_partitions=2)
        languages=out/'languages.csv'
        languages.write_text('tconst,language,source_url\ntt0000001,en,https://example.invalid/synthetic\n')
        config={'minimum_runtime':40,'minimum_votes':1,'episode_minimum_votes':1,
                'runtime_zscore':2,'success_rating':7,'early_career_fraction':0.25,
                'student_validated':False,'as_of_year':2026}
        explore(spark, prepared, analysis, config, languages)
        reference=out/'raw-reference.csv'
        reference.write_text('wiki,title,imdb_id,wikidata_id\nenwiki,Test_Title,tt0000001,Q1\nenwiki,___,tt0000001,Q1\nenwiki,Unknown,tt9999999,Q99\n')
        export(spark, prepared, exports, selected_titles=analysis/'selected_titles.csv',
               wiki_reference=reference, reference_source='https://example.invalid/synthetic')
        # Use the actual producer's exact file, never hand-rebuild its contents.
        candidates=exports/'candidates.csv'
        pairs=list(read_ids(candidates,CANDIDATE_COLUMNS))
        assert pairs == [('tt0000001','nm0000001')], pairs
        report['assertions'].append('Exported candidate CSV accepted by Benjamin strict reader')
        table=reference_table(spark,str(exports/'streaming_reference.csv'))
        rows=table.collect()
        assert len(rows)==1 and rows[0].title_key=='Test Title'
        assert rows[0].imdb_matches[0].imdb_id=='tt0000001'
        rejects=spark.read.parquet(str(exports/'rejected_reference')).count()
        assert rejects==2, rejects
        report['assertions'].append('Streaming consumer accepts normalized producer CSV; blank and absent IDs rejected')
        cache=out/'synthetic-cache.json'
        records=[]
        for ident,section in [('tt0000001','Plot'),('nm0000001','Lead')]:
            records.append({'imdb_id':ident,'wikidata_url':'https://example.invalid/Q1',
                'page_title':'SYNTHETIC TEST ONLY','source_url':'https://example.invalid/revision',
                'retrieved_at_utc':'2026-11-11T00:00:00Z','section':section,
                'html':'<div class="mw-parser-output"><p>Synthetic contract test text.</p></div>',
                'missing_reason':None})
        cache.write_text(''.join(json.dumps(r)+'\n' for r in records))
        enriched=out/'enriched'
        write_results(spark,str(candidates),str(cache),str(enriched),str(analysis/'selected_titles.csv'),
                      str(candidates),str(analysis/'top10_per_genre_year.csv'))
        manifest=json.loads((enriched/'_MANIFEST.json').read_text())
        assert manifest['films_requested']==manifest['people_requested']==manifest['candidate_pairs']==1
        assert manifest['films_by_status']=={'available':1}
        assert manifest['people_by_status']=={'available':1}
        assert manifest['films_with_three_biographies']==0
        assert manifest['films_with_fewer_than_three_candidates']==1
        assert manifest['collection_complete'] and (enriched/'_COMPLETE').exists()
        report['assertions'].append('Producer selection and top CSV feed Spark enrichment with exact coverage; complete cache does not imply three biographies')
        module_path=ROOT/'project/collaborations/analyse.py'
        spec=importlib.util.spec_from_file_location('collab_audit',module_path)
        collab=importlib.util.module_from_spec(spec);spec.loader.exec_module(collab)
        result=collab.run(spark,prepared/'clean',out/'collaborations',SimpleNamespace(
            input_manifest=prepared/'input_manifest.json',scope='fixture',include_self=False,
            start_year=1980,end_year=2025,min_votes=[0,1000],min_history_years=[2]))
        assert len(result['comparisons'])==2
        assert all(r['pairs']==0 for r in result['comparisons'])
        report['assertions'].append('Collaborations consumes the actual seven-table producer schemas and handles insufficient history')
        report.update(status='passed',enrichment=manifest,streaming_reference_rows=len(rows),
                      rejected_reference_rows=rejects,collaborations=result)
    except BaseException as error:
        report.update(status='failed',error=f'{type(error).__name__}: {error}')
        raise
    finally:
        report['completed_at_utc']=datetime.now(timezone.utc).isoformat()
        (out/'audit.json').write_text(json.dumps(report,indent=2)+'\n')
        spark.stop()

if __name__=='__main__':
    main()
