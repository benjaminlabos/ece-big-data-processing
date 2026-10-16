. "$PSScriptRoot/env.ps1"
& $Minikube -p $Profile stop
if ($LASTEXITCODE -ne 0) { throw 'Arrêt minikube impossible.' }
