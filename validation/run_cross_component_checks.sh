#!/usr/bin/env bash
# Native equivalent of the Docker launcher; use an isolated Python environment.
set -euo pipefail
review_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$review_root"
review_python="${GR04_PYTHON:-python}"
if [[ -e results/cross-pipeline-v1 || -e results/query-review-fixture ]]; then
  echo 'Use a clean clone: existing controlled outputs will not be overwritten.' >&2
  exit 1
fi
mkdir -p results
export PYTHONPATH="$review_root:$review_root/project/enrichment:$review_root/05.structured-streaming/lab_docker_wikistreams/jobs${PYTHONPATH:+:$PYTHONPATH}"
export PYSPARK_PYTHON="$review_python"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS='--master local[2] --driver-memory 1g --conf spark.ui.showConsoleProgress=false pyspark-shell'
"$review_python" -m unittest discover -s project/enrichment -p test_inputs.py > results/enrichment-unit-review.log 2>&1
"$review_python" validation/check_cross_pipeline.py > results/cross-pipeline-review.log 2>&1
"$review_python" validation/check_query_review.py > results/query-review-review.log 2>&1
echo 'Controlled cross-component checks passed; inspect results/ for evidence.'
