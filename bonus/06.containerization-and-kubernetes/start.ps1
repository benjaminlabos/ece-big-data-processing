. "$PSScriptRoot/env.ps1"
& $Minikube start -p $Profile --driver=docker --cpus=2 --memory=3072 --kubernetes-version=v1.35.0
if ($LASTEXITCODE -ne 0) { throw 'Le cluster minikube ne démarre pas.' }
& $Minikube -p $Profile addons enable metrics-server
if ($LASTEXITCODE -ne 0) { throw 'Activation de metrics-server impossible.' }
kube get nodes
