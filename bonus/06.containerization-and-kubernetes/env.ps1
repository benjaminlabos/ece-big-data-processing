$ErrorActionPreference = 'Stop'
$LabRoot = $PSScriptRoot
$env:MINIKUBE_HOME = Join-Path $LabRoot '.runtime/minikube-home'
$env:KUBECONFIG = Join-Path $LabRoot '.runtime/kubeconfig'
$Minikube = Join-Path $LabRoot '.runtime/minikube.exe'
$Profile = 'ece-tp06'
$Kubectl = Join-Path $env:MINIKUBE_HOME '.minikube/cache/windows/amd64/v1.35.0/kubectl.exe'
Set-Alias -Name kube -Value $Kubectl -Scope Global
