param(
    [string]$Snapshot = 'data/raw/imdb-full-2026-11-11',
    [string]$PreparedOutput = 'results/imdb-full-2026-11-11',
    [string]$AnalysisOutput = 'results/exploration-full-provisional-2026-11-11',
    [string]$Configuration = 'examples/exploration/provisional.json',
    [string]$OriginalLanguages = 'data/reference/original-languages-2026-11-11.csv',
    [string]$ResumeFrom = '',
    [string]$RawProfile = '',
    [int]$LegacyAsOfYear = 0
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Get-Location).Path
if (-not (Test-Path (Join-Path $Snapshot 'manifest.json'))) { throw 'Complete input manifest required' }
if ((Test-Path $PreparedOutput) -or (Test-Path $AnalysisOutput) -or (Test-Path ($AnalysisOutput + '-credits'))) { throw 'Choose new output directories' }
$manifest = Get-Content -Raw (Join-Path $Snapshot 'manifest.json') | ConvertFrom-Json
if ($manifest.scope -ne 'full' -or @($manifest.files.PSObject.Properties).Count -ne 7) { throw 'Expected seven complete IMDb files' }
foreach ($entry in $manifest.files.PSObject.Properties) {
    if (-not $entry.Value.gzip_integrity_verified) { throw 'Input gzip integrity not verified' }
}
docker compose -f compose.preparation.yaml -f compose.full.yaml config -q
if ($LASTEXITCODE -ne 0) { throw 'Full run Compose configuration invalid' }
$driveName = (Get-Item $repoRoot).PSDrive.Name
if ((Get-PSDrive $driveName).Free -lt 12GB) { throw 'At least 12 GiB free required before launch; actual peak remains to measure' }
if ($ResumeFrom -and (-not (Test-Path ($ResumeFrom + '/progress.json')) -or -not $RawProfile)) { throw 'Recovery requires checkpoint progress and a completed raw profile' }
if ($RawProfile -and -not (Test-Path ($RawProfile + '/PROFILE_SUCCESS.json'))) { throw 'Completed raw profile marker missing' }
if ($ResumeFrom -and (Test-Path results/imdb-full-prepare.log)) {
    $archiveName = 'results/imdb-full-prepare-before-recovery-' + [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss') + '.log'
    Copy-Item results/imdb-full-prepare.log $archiveName
}
$worker = Start-Job -ArgumentList $repoRoot,$Snapshot,$PreparedOutput,$AnalysisOutput,$Configuration,$OriginalLanguages,$ResumeFrom,$RawProfile,$LegacyAsOfYear -ScriptBlock {
    param($rootPath,$snapshotPath,$preparedPath,$analysisPath,$configurationPath,$languagePath,$checkpointPath,$profilePath,$legacyYear)
    Set-Location $rootPath
    $ErrorActionPreference = 'Continue'
    $preparationArguments = @('--input-partitions','64')
    if ($profilePath) { $preparationArguments += @('--raw-profile',$profilePath) }
    if ($checkpointPath) { $preparationArguments += @('--resume-from',$checkpointPath,'--legacy-as-of-year',[string]$legacyYear) }
    docker compose -f compose.preparation.yaml -f compose.full.yaml run --rm --name gr04-imdb-full-run preparation python -m imdb_preparation.prepare --raw-dir $snapshotPath --output $preparedPath --master 'local[4]' --shuffle-partitions 64 --as-of-year 2026 @preparationArguments *> results/imdb-full-prepare.log
    if ($LASTEXITCODE -ne 0) { throw 'Full preparation failed; inspect results/imdb-full-prepare.log and partial outputs' }
    $languageArguments = @()
    if (Test-Path $languagePath) { $languageArguments = @('--original-languages',$languagePath) }
    docker compose -f compose.preparation.yaml -f compose.full.yaml run --rm --name gr04-imdb-explore-run preparation python -m imdb_preparation.explore --prepared-dir $preparedPath --output $analysisPath --config $configurationPath --shuffle-partitions 64 @languageArguments *> results/imdb-full-exploration.log
    if ($LASTEXITCODE -ne 0) { throw 'Exploration failed; prepared tables remain available' }
    docker compose -f compose.preparation.yaml -f compose.full.yaml run --rm --name gr04-imdb-exports-run preparation python -m imdb_preparation.exports --prepared-dir $preparedPath --output ($analysisPath + '-credits') --selected-titles ($analysisPath + '/selected_titles.csv') *> results/imdb-full-credit-export.log
    if ($LASTEXITCODE -ne 0) { throw 'Credit export failed; analysis and prepared tables remain available' }
    Write-Output 'Full preparation and provisional exploration completed'
}
Write-Output ('Started full preparation and exploration; worker job ' + $worker.Id)
try {
    while ($worker.State -eq 'Running' -or $worker.State -eq 'NotStarted') {
        if ((Get-PSDrive $driveName).Free -lt 5GB) {
            docker stop gr04-imdb-full-run gr04-imdb-explore-run gr04-imdb-exports-run 2>$null
            Stop-Job $worker
            throw 'Stopped this task below 5 GiB free; raw inputs and partial outputs preserved'
        }
        Start-Sleep -Seconds 10
    }
    Receive-Job $worker
    if ($worker.State -ne 'Completed') { throw 'Full run worker failed; inspect logs' }
} finally {
    if ($worker.State -eq 'Completed' -or $worker.State -eq 'Failed' -or $worker.State -eq 'Stopped') { Remove-Job $worker }
}
