$ErrorActionPreference = 'Stop'
$runtime = Join-Path $PSScriptRoot '.runtime'
New-Item -ItemType Directory -Path $runtime -Force | Out-Null
$binary = Join-Path $runtime 'minikube.exe'
if (-not (Test-Path -LiteralPath $binary)) {
    $url = 'https://storage.googleapis.com/minikube/releases/v1.39.0/minikube-windows-amd64.exe'
    Invoke-WebRequest $url -OutFile $binary
    $checksumFile = Join-Path $runtime 'minikube.exe.sha256'
    Invoke-WebRequest "$url.sha256" -OutFile $checksumFile
    $expected = (Get-Content $checksumFile -Raw).Trim()
    $actual = (Get-FileHash $binary -Algorithm SHA256).Hash.ToLower()
    if ($actual -ne $expected) { throw 'Checksum minikube incorrect.' }
}
# Recrée seulement les deux fichiers locaux ignorés par Git, avec les exemples
# fictifs des consignes. Aucun identifiant personnel n'est lu ni nécessaire.
$courseCache = Join-Path $runtime 'course'
New-Item -ItemType Directory -Path $courseCache -Force | Out-Null
$instructionsFile = Join-Path $courseCache 'lab-2.md'
if (-not (Test-Path -LiteralPath $instructionsFile)) {
    $courseRevision = '2aa3d2030b598b16369a39105bb9b094d7ed56f6'
    $courseUrl = "https://raw.githubusercontent.com/adaltas/ece-big-data-processing-fall-2026/$courseRevision/06.containerization-and-kubernetes/lab-2.md"
    Invoke-WebRequest $courseUrl -OutFile $instructionsFile
}
$instructions = Get-Content $instructionsFile -Raw
foreach ($name in @('s3-credentials.txt', 'secret-ceph-credentials.yaml')) {
    $pattern = "cat > " + [regex]::Escape($name) + " << 'EOF'\r?\n(.*?)\r?\nEOF"
    $match = [regex]::Match($instructions, $pattern, 'Singleline')
    if (-not $match.Success) { throw "Exemple introuvable : $name" }
    $destination = Join-Path $PSScriptRoot "lab-2/$name"
    if (-not (Test-Path -LiteralPath $destination)) {
        [IO.File]::WriteAllText($destination, $match.Groups[1].Value.Replace("`r`n", "`n") + "`n", [Text.UTF8Encoding]::new($false))
    }
}
& $binary version
