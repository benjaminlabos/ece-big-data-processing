# Chapitre 06 — Réponses aux labs

## Lab 1 — Bases de Kubernetes

### Métriques et composants

`kubectl top pods -A --sort-by=cpu --sum=true` affiche la consommation CPU et
mémoire mesurée des Pods de tous les namespaces, trie les lignes par CPU et
ajoute une somme. Il ne montre pas leurs réservations (`requests`) ou leurs
plafonds (`limits`). Les métriques nécessitent `metrics-server` et un premier
échantillon de mesure.

Dans `kube-system` :

| Composant | Rôle |
| --- | --- |
| kube-apiserver | Expose l'API Kubernetes et valide les opérations. |
| etcd | Conserve l'état du cluster. |
| kube-scheduler | Choisit le nœud auquel affecter chaque nouveau Pod. |
| kube-controller-manager | Réconcilie l'état réel et l'état souhaité. |
| kube-proxy | Configure le routage des Services sur le nœud. |
| CoreDNS | Résout les noms des Services et les noms internes. |
| kindnet | Met en place la connectivité réseau entre les Pods dans ce profil minikube. |
| metrics-server | Collecte les mesures nécessaires à `kubectl top`. |
| storage-provisioner | Fournit le stockage dynamique local de minikube. |

Le kubelet et containerd s'exécutent comme services du nœud, et non comme Pods
à retrouver dans cette liste.

### Application et exposition

Le code de l'application est `/server.js` et son port HTTP est **8080**.
`kubectl exec <pod> -- curl -s localhost:8080` interroge l'application dans son
conteneur. Sur Windows avec le pilote Docker, l'adresse du Pod et celle du
nœud ne sont généralement pas directement routables depuis l'hôte. Un Service
NodePort fournit un point d'entrée stable ; `minikube service <service> --url`
crée le tunnel local utilisé pour les requêtes depuis Windows. Le processus
du tunnel doit rester ouvert.

Un `kubectl port-forward service/...` atteint un Pod sélectionné : il convient
pour tester l'accès, mais pas pour démontrer la répartition entre répliques.
Les essais utilisent donc le tunnel vers le **NodePort**.

### Réplication

`kubectl scale deployment/kubernetes-bootcamp --replicas=5` demande cinq Pods.
On vérifie le résultat avec `kubectl get pods -l app=kubernetes-bootcamp`, en
attendant cinq Pods `Running` et `Ready 1/1`.

Des requêtes HTTP ouvrant de nouvelles connexions peuvent atteindre différents
Pods : le Service répartit les connexions entre ses endpoints prêts. Dix
requêtes ne garantissent pas de visiter toutes les répliques. Un navigateur
qui réutilise une connexion peut continuer à atteindre le même Pod.

Après `--replicas=2`, le contrôleur supprime trois Pods. Il faut attendre la fin
de leur terminaison pour constater exactement deux Pods.

### Rolling update et retour arrière

Lors du passage de v1 à v2, un nouveau ReplicaSet démarre les Pods v2 et réduit
progressivement les Pods v1. Les réponses évoluent donc de `v=1` vers `v=2`.
`kubectl rollout status deployment/kubernetes-bootcamp` confirme la fin du
déploiement. Les observations HTTP de cette exécution sont conservées dans
`results/lab-1-http.json` ; elles mesurent des requêtes espacées de 0,5 seconde
et ne constituent pas une garantie générale d'absence d'interruption.

L'image v3 volontairement inexistante provoque `ErrImagePull`, puis
`ImagePullBackOff` : le nouveau Pod ne devient pas prêt. La mise à jour reste
bloquée, mais les anciens Pods v2 continuent à répondre.

`kubectl rollout undo deployment/kubernetes-bootcamp` annule la tentative v3.
Pour revenir explicitement à l'image initiale, on consulte d'abord
`kubectl rollout history deployment/kubernetes-bootcamp`, puis on utilise
`kubectl rollout undo deployment/kubernetes-bootcamp --to-revision=1` dans
cet exercice commencé sur un Deployment neuf. Les révisions ne doivent pas
être supposées identiques dans un Deployment déjà utilisé.

### Manifests

`lab-1/deployment.yaml` contient l'image v1, le port 8080, le label
`app: kubernetes-bootcamp` et **3 répliques**. `lab-1/service.yaml` sélectionne
ce même label, expose le port 8080 et dirige les connexions vers le port 8080
des conteneurs. La validation applique d'abord le Deployment à une réplique,
puis réapplique le fichier final à trois répliques.

## Lab 2 — ConfigMap et Secret

Les trois méthodes de création sont réalisées : fichier local, valeurs
littérales et manifest YAML. Un ConfigMap conserve des paramètres publics ;
un Secret conserve des données sensibles, avec contrôle d'accès adapté.
Le base64 du champ `data` est réversible et **ne chiffre pas** les valeurs.

Les clés d'accès utilisées ici sont uniquement les exemples fictifs fournis
dans l'énoncé. Elles ne permettent pas de vérifier une connexion Ceph/S3 :
aucun serveur Ceph, bucket ou stockage externe n'est déployé dans ce lab.
Les deux fichiers locaux de credentials sont ignorés par Git.

- `pipeline-config` est créé depuis `pipeline-config.properties`.
- `data-paths` est créé avec `--from-literal`.
- `data-platform-config` est appliqué depuis son YAML.
- `ceph-credentials`, `s3-credentials` et `ceph-s3-credentials` illustrent les
  trois méthodes équivalentes pour les Secrets.
- `spark-app` reçoit `LOG_LEVEL=INFO` et `SPARK_WORKERS=4` via `valueFrom`,
  ainsi que les clés via `envFrom`.
- `ceph-connector` reçoit les clés du Secret et termine avec succès. L'affichage
  de la clé fictive dans ses logs suit la démonstration du cours.
- `spark-config-reader` expose `application.yaml` et `tables.csv` sous
  `/etc/config`. Le champ `items` sélectionne et renomme les clés projetées.
- `ceph-connector-secure` monte le fichier de credentials en lecture seule.
  `defaultMode: 0600` correspond à la valeur décimale 384 et aux permissions
  lecture/écriture pour le propriétaire ; le montage reste en lecture seule.
  Kubernetes projette le contenu par des liens symboliques : utiliser
  `stat -L -c '%a' /etc/ceph-credentials/s3-credentials.txt` pour contrôler les
  permissions du fichier cible plutôt que les `777` du lien symbolique.

Une variable d'environnement déjà injectée ne suit pas automatiquement les
modifications du ConfigMap ou Secret : le Pod doit être recréé. Un volume
projeté peut être mis à jour ultérieurement ; l'application doit relire son
fichier. Les montages avec `subPath` ont un comportement différent.

En production : limiter les droits RBAC, chiffrer le stockage des Secrets,
éviter de les écrire dans les logs et employer un coffre ou des manifests
chiffrés. Les namespaces du lab sont supprimés après les vérifications, mais
les fichiers de travail locaux sont conservés pour reprendre les exercices.

## Lab 3 — Job et CronJob

### Exécution simple et échecs

Un Job vise un nombre d'exécutions terminées avec succès ; un CronJob crée
des Jobs suivant une expression cron. Les messages d'import et d'ETL sont
des **simulations Python de l'énoncé**, pas un traitement de données réelles.

Avec `restartPolicy: Never` et `backoffLimit: 3`, le Job de transformation qui
retourne systématiquement le code 1 crée **4 Pods** : une première tentative
et trois reprises, espacées par un backoff exponentiel. Le motif final est
`BackoffLimitExceeded`. Les Pods terminés sont conservés pour examiner leurs
codes de sortie, logs et événements, tant qu'aucun nettoyage ne les supprime.

Avec `activeDeadlineSeconds: 10`, la durée totale autorisée expire avant la
fin des reprises : le motif devient `DeadlineExceeded`. Cette limite inclut
les attentes et le téléchargement des images, d'où le préchargement de Python.
Le fichier `job-with-deadline.yaml` utilise un nom distinct pour conserver
simultanément les preuves des deux variantes.

### Parallélisation

`parallelism: 4` autorise quatre Pods simultanés, `completions: 4` exige quatre
succès. Avec `completionMode: Indexed`, `JOB_COMPLETION_INDEX` prend les valeurs
0, 1, 2 et 3 ; chaque Pod traite son shard logique, défini par `id % 4`.
La valeur `completedIndexes: 0-3` atteste des quatre succès. Le TTL de 600
secondes permet ensuite de nettoyer le Job et ses Pods automatiquement.

### Planification

| Expression | Fréquence |
| --- | --- |
| `*/2 * * * *` | Toutes les 2 minutes. |
| `*/5 * * * *` | Toutes les 5 minutes. |
| `*/15 * * * *` | Toutes les 15 minutes. |
| `0 * * * *` | Toutes les heures à la minute 0. |
| `0 2 * * *` | Chaque jour à 2 heures. |
| `0 3 * * 1` | Chaque lundi à 3 heures. |
| `0 4 1 * *` | Le premier jour du mois à 4 heures. |

`daily-backup` utilise `timeZone: Europe/Paris`, donc 2 heures locales. Sans ce
champ, la planification utilise le fuseau du contrôleur Kubernetes, souvent
UTC. `concurrencyPolicy: Forbid` empêche deux Jobs du même CronJob de se
chevaucher. Les limites d'historique conservent ici au plus trois Jobs réussis
et un Job échoué de `bronze-to-silver-pipeline`.

La validation attend un vrai Job créé automatiquement par le CronJob toutes
les deux minutes et consulte ses logs. Elle teste aussi `suspend: true`, puis
`false`. Suspendre ne stoppe pas un Job déjà lancé.

### Scénario ETL

`etl-config` fournit `BATCH_SIZE=1000`, `SOURCE_LAYER=bronze` et
`TARGET_LAYER=silver`. Le CronJob `etl-bronze-to-silver` injecte ce ConfigMap
avec `envFrom`. Il demande 100 milli-CPU et 64 MiB de mémoire, avec une limite
mémoire de 128 MiB. Un Job manuel créé avec
`kubectl create job etl-manual-run --from=cronjob/etl-bronze-to-silver` permet de
tester immédiatement le template. Ce lancement manuel ne prouve pas à lui
seul le déclenchement planifié de ce CronJob toutes les cinq minutes.

Les logs, statuts et vérifications de chaque lab sont conservés sous `results/`.

## Résultats de l'exécution du 16 octobre 2026

- **Lab 1** : les 30 requêtes à cinq répliques ont atteint les cinq Pods ; les
  30 requêtes après application du manifest final ont atteint les trois Pods.
  Pendant les mises à jour et retours arrière, **44 requêtes HTTP, 0 erreur**,
  avec observation des versions v1 et v2. L'image v3 a produit `ErrImagePull`.
- **Lab 2** : les trois ConfigMaps et trois Secrets ont été créés ; les
  variables, le décodage base64, les deux fichiers de configuration et le
  fichier de credentials fictifs concordent avec les valeurs attendues.
  Les permissions du fichier cible sont bien **600**.
- **Lab 3** : import terminé, quatre Pods pour les reprises, motifs
  `BackoffLimitExceeded` et `DeadlineExceeded` constatés. Le Job parallèle a
  terminé les index **0-3**. L'ETL manuel a reçu `bronze`, `silver` et `1000` ;
  le template de backup a aussi terminé avec succès lors d'un lancement
  manuel. Le Job **bronze-to-silver-pipeline-29869278**, créé automatiquement
  par le CronJob, a terminé avec le message de simulation « 500 records
  processed ». La suspension puis la reprise ont été vérifiées dans l'API.

Les dates détaillées des fichiers JSON sont en UTC. Les fichiers locaux sont
conservés ; les ressources Kubernetes des exercices ont été nettoyées.
