# Bonus — Conteneurisation et Kubernetes (chapitre 06)

**Travail facultatif, hors remise officielle.** Ces exercices ont été réalisés
pour approfondir le cours.

Les trois labs sont réalisés localement dans `lab-1/`, `lab-2/` et `lab-3/`.
Les réponses sont dans [REPONSES.md](REPONSES.md), les traces d'exécution et les
contrôles dans [results/](results/).

## Fichiers

- `lab-1/` : Deployment complété avec 3 répliques et Service NodePort.
- `lab-2/` : configuration, Pods et volumes ; les Secrets emploient exclusivement
  les identifiants fictifs de l'énoncé.
- `lab-3/` : Job simple, reprises, deadline, Job indexé, CronJobs et ConfigMap ETL.
- `run_labs.py` : réalisation des manipulations, assertions sur les résultats,
  sauvegarde des commandes et sorties. Python standard, sans paquet à installer.
- `results/` : résultats synthétiques des trois labs et échantillons HTTP.

Les consignes et illustrations sont consultables dans le dépôt du cours.

[Source du cours](https://github.com/adaltas/ece-big-data-processing-fall-2026/tree/main/06.containerization-and-kubernetes),
révision consultée : `2aa3d2030b598b16369a39105bb9b094d7ed56f6`.

## Validation réelle du 16 octobre 2026

| Lab | Résultats constatés |
| --- | --- |
| [Lab 1](results/lab-1.json) | 5 puis 2 répliques ; trafic sur les 5 Pods ; v2, image v3 en échec et deux retours arrière vérifiés ; 44 requêtes suivies, 0 erreur HTTP ; manifests à 3 répliques, toutes atteintes. |
| [Lab 2](results/lab-2.json) | 3 ConfigMaps et 3 Secrets ; variables injectées ; 2 fichiers de configuration montés ; credentials fictifs projetés avec permissions `0600`. |
| [Lab 3](results/lab-3.json) | Import réussi ; 4 tentatives avant `BackoffLimitExceeded` ; variante à 10 s avec `DeadlineExceeded` ; shards 0 à 3 réussis ; ETL et backup manuels réussis ; Job automatiquement planifié observé ; suspension/reprise vérifiée. |

Les commandes et sorties complètes sont conservées uniquement en local dans
`results/lab-1.txt`, `results/lab-2.txt` et `results/lab-3.txt` ; le script les
recrée lors d'une nouvelle exécution. Les échecs de v3 et des Jobs de
transformation sont intentionnels et font partie des exercices. Les workloads
des trois labs et les deux namespaces dédiés ont été supprimés après validation.

## Relancer sous PowerShell

Prérequis : Docker Desktop lancé en mode conteneurs Linux, Python 3 et accès
Internet au premier démarrage. Depuis la racine du dépôt :

```powershell
cd bonus/06.containerization-and-kubernetes
./setup.ps1
./start.ps1
python -X utf8 ./run_labs.py all
./stop.ps1
```

Les scripts utilisent minikube **v1.39.0**, Kubernetes **v1.35.0**, 2 CPU et
3 Gio de RAM. Le profil s'appelle `ece-tp06`. Le binaire, le kubeconfig et le
cache minikube restent dans `.runtime/`, ignoré par Git ; le kubeconfig global
du PC n'est pas modifié. Le premier démarrage télécharge plusieurs images.

Pour rejouer un seul lab :

```powershell
python -X utf8 ./run_labs.py 1
python -X utf8 ./run_labs.py 2
python -X utf8 ./run_labs.py 3
```

Chaque exécution réussie supprime ses workloads en fin de lab. Les fichiers de
travail restent disponibles. Après une exécution interrompue, vérifier les
objets existants avant de relancer : des objets homonymes peuvent encore être
présents dans ce profil dédié. Le script conserve les traces d'échec pour le
diagnostic ; un fichier JSON `passed` correspond seulement à la dernière
exécution réussie, dont la date figure dans le fichier.

## Manipulation manuelle

Charger les variables et l'alias `kube` dans chaque nouveau terminal :

```powershell
. ./env.ps1
kube get nodes
kube -n kube-system get pods
kube top pods -A --sort-by=cpu --sum=true
```

`kube` est un alias du kubectl fourni par minikube, avec son
kubeconfig local. Les commandes de l'énoncé utilisant `kubectl` peuvent ainsi
être exécutées avec `kube` sous PowerShell. Les substitutions Bash comme
`export X=$(...)`, les heredocs, `grep` et `base64 -d` sont remplacées dans le
script Python par des appels compatibles Windows et la lecture JSON.

Exemple déclaratif du lab 1 :

```powershell
kube apply -f ./lab-1/deployment.yaml
kube apply -f ./lab-1/service.yaml
kube rollout status deployment/kubernetes-bootcamp
& $Minikube -p $Profile service kubernetes-bootcamp-service --url
```

Conserver ce terminal ouvert ; interroger l'URL locale affichée dans un autre
terminal. Une fois terminé, fermer le tunnel avec Ctrl+C, puis nettoyer :

```powershell
kube delete -f ./lab-1/service.yaml -f ./lab-1/deployment.yaml
./stop.ps1
```

`setup.ps1` télécharge les consignes du lab 2 à la révision indiquée dans un
cache local ignoré par Git, puis recrée les deux fichiers de credentials
fictifs nécessaires. Ils ne sont pas versionnés.

Les logs locaux du lab 2 contiennent les **valeurs fictives de l'énoncé** pour montrer
le décodage et la projection des Secrets. Les scripts n'utilisent aucun
identifiant personnel. Les labs 2 et 3 ne déploient pas Ceph, S3, Kafka ou
Spark : ils montrent l'injection de configuration et des tâches Python
simulées, conformément aux consignes.
