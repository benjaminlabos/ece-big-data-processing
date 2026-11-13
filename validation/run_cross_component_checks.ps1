param([string]$Image = 'tp05-wikistreams-notebook:local')
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
if (Test-Path results/cross-pipeline-v1) { throw 'Use a clean clone or a new isolated worktree; results/cross-pipeline-v1 already exists.' }
if (Test-Path results/query-review-fixture) { throw 'Review output already exists; use a clean clone.' }
New-Item -ItemType Directory -Force results | Out-Null
$mount = "type=bind,source=$root,target=/job"
$envPath = '/job:/job/project/enrichment:/job/05.structured-streaming/lab_docker_wikistreams/jobs'
foreach ($script in @('check_cross_pipeline.py','check_query_review.py')) {
    $savedErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    & docker run --rm --pull=never --label gr04.audit=clean-clone --cpus 2 --memory 2500m --mount $mount -w /job `
        -e "PYTHONPATH=$envPath" -e PYSPARK_PYTHON=python -e SPARK_LOCAL_IP=127.0.0.1 `
        -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 1g pyspark-shell' $Image python "validation/$script"
    $runExitCode = $LASTEXITCODE
    $ErrorActionPreference = $savedErrorPreference
    if ($runExitCode) { throw "Cross-component audit failed: $script (exit $runExitCode)" }
}
'Controlled cross-component checks passed. Real full IMDb results still require validation.'
