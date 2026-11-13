param(
    [string]$Snapshot = 'data/raw/imdb-full-2026-11-11',
    [string]$PreparedOutput = 'results/imdb-full-2026-11-11',
    [string]$AnalysisOutput = 'results/exploration-full-provisional-2026-11-11',
    [string]$RawProfile = 'results/imdb-full-raw-profile-2026-11-11',
    [string]$Output = 'validation/full-analysis-windows-2026-11-11.json',
    [int]$WaitForRunnerPid = 0,
    [switch]$Publish
)
$ErrorActionPreference = 'Stop'
function Read-Json([string]$Path) {
    $parsed = Get-Content -Raw -Encoding UTF8 $Path | ConvertFrom-Json
    # Explicit enumeration avoids PowerShell 5.1's array value/Count JSON wrapper.
    foreach ($item in $parsed) { Write-Output $item }
}
function File-Sha([string]$Path) { (Get-FileHash $Path -Algorithm SHA256).Hash.ToLower() }
if (Test-Path $Output) { throw 'Evidence output already exists; choose another path' }
$creditsPath = $AnalysisOutput + '-credits'
$markers = @(($PreparedOutput + '/SUCCESS.json'), ($AnalysisOutput + '/SUCCESS.json'), ($creditsPath + '/SUCCESS.json'))
while (@($markers | Where-Object { -not (Test-Path $_) }).Count -gt 0) {
    if ($WaitForRunnerPid -le 0) { throw 'Full pipeline is incomplete' }
    if (-not (Get-Process -Id $WaitForRunnerPid -ErrorAction SilentlyContinue)) {
        throw 'Runner ended without every success marker; inspect results/imdb-full-*.log'
    }
    Start-Sleep -Seconds 20
}
$prepared = Read-Json ($PreparedOutput + '/summary.json')
$analysis = Read-Json ($AnalysisOutput + '/summary.json')
$credits = Read-Json ($creditsPath + '/summary.json')
foreach ($summary in @($prepared, $analysis, $credits)) {
    if ($summary.scope -ne 'full') { throw 'Full-scope outputs required' }
}
if (@($prepared.tables.PSObject.Properties).Count -ne 7) { throw 'Expected seven prepared tables' }
if (@($analysis.questions.PSObject.Properties).Count -ne 9) { throw 'Expected eight questions and ranking' }
foreach ($question in $analysis.questions.PSObject.Properties) {
    if ($question.Value.status -notin @('computed', 'provisional')) { throw ('Question incomplete: ' + $question.Name) }
}
if (-not $credits.enrichment.consumer_ready) { throw 'No usable enrichment credits' }
$manifest = Read-Json ($Snapshot + '/manifest.json')
$preparedManifest = Read-Json ($PreparedOutput + '/input_manifest.json')
foreach ($table in $manifest.files.PSObject.Properties) {
    if ($table.Value.sha256 -ne $preparedManifest.files.($table.Name).sha256) { throw 'Input manifest mismatch' }
}
$columnProfiles = @(Read-Json ($PreparedOutput + '/missing_profile.json'))
if ($columnProfiles.Count -ne 39) { throw 'Expected 39 column profiles' }
if (Test-Path ($RawProfile + '/PROFILE_SUCCESS.json')) {
    $independent = Read-Json ($RawProfile + '/summary.json')
    if ($independent.manifest_sha256 -ne (File-Sha ($Snapshot + '/manifest.json'))) { throw 'Raw profile source mismatch' }
    $byColumn = @{}
    foreach ($entry in @(Read-Json ($RawProfile + '/missing_profile.json'))) { $byColumn[$entry.table + '.' + $entry.column] = $entry }
    if ($byColumn.Count -ne 39) { throw 'Independent profile incomplete' }
    foreach ($entry in $columnProfiles) {
        $other = $byColumn[$entry.table + '.' + $entry.column]
        if ($null -eq $other -or $entry.total_rows -ne $other.total_rows -or $entry.missing_rows -ne $other.missing_rows) {
            throw 'Independent and preparation column profiles differ'
        }
    }
}
$previews = [ordered]@{}
foreach ($file in Get-ChildItem ($AnalysisOutput + '/previews/*.json')) { $previews[$file.BaseName] = @(Read-Json $file.FullName) }
if ($previews.Count -ne 9) { throw 'Expected nine bounded query previews' }
$evidence = [ordered]@{
    execution = 'windows_docker'
    captured_at = [DateTime]::UtcNow.ToString('o')
    collection_commit = (git rev-parse HEAD)
    input_manifest_sha256 = (File-Sha ($Snapshot + '/manifest.json'))
    preparation = $prepared
    missing_profile = $columnProfiles
    top5_missing = @(Read-Json ($PreparedOutput + '/top5_missing.json'))
    exploration = $analysis
    query_previews = $previews
    integration_exports = $credits
    files = [ordered]@{
        selected_titles = [ordered]@{path = $AnalysisOutput + '/selected_titles.csv'; sha256 = (File-Sha ($AnalysisOutput + '/selected_titles.csv'))}
        ranking = [ordered]@{path = $AnalysisOutput + '/top10_per_genre_year.csv'; sha256 = (File-Sha ($AnalysisOutput + '/top10_per_genre_year.csv'))}
        candidates = [ordered]@{path = $creditsPath + '/candidates.csv'; sha256 = (File-Sha ($creditsPath + '/candidates.csv'))}
    }
}
[IO.Directory]::CreateDirectory((Split-Path (Join-Path (Get-Location) $Output))) | Out-Null
[IO.File]::WriteAllText((Join-Path (Get-Location) ($Output + '.part')), ($evidence | ConvertTo-Json -Depth 40), [Text.UTF8Encoding]::new($false))
Move-Item ($Output + '.part') $Output
Write-Output ('Verified complete pipeline; evidence: ' + $Output)
if ($Publish) {
    if ((git branch --show-current) -ne 'feat/imdb-full-analysis') { throw 'Branch changed; evidence retained without publishing' }
    git add -- $Output
    if ($LASTEXITCODE) { throw 'Evidence staging failed' }
    git -c user.name='Nicolas Rihana' -c user.email='nrihana1@gmail.com' commit --only -m 'Record verified full IMDb preparation and analysis outputs' -- $Output
    if ($LASTEXITCODE) { throw 'Evidence commit failed' }
    git push origin HEAD:refs/heads/feat/imdb-full-analysis
    if ($LASTEXITCODE) { throw 'Evidence push failed; local evidence preserved' }
}
