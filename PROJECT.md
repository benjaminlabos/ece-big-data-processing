# Big Data Processing — gr-04

Guide technique de lancement. Les explications, choix méthodologiques et
interprétations du rapport sont à écrire par les étudiants dans `REPORT.md`.
Ce guide couvre uniquement la première tâche de préparation ; les autres parties
du projet restent à réaliser sur leurs branches respectives.

## Contributeurs

| Nom | GitHub | E-mail |
| --- | --- | --- |
| Nicolas Rihana | nicohris | nrihana1@gmail.com |
| Benjamin Labos | benjaminlabos | benjamin.labos@edu.ece.fr |

## Références vérifiées le 10 novembre 2026

- [Énoncé officiel](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/project_instructions.md).
- [Soumission](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/README.md).
- [Documentation IMDb](https://developer.imdb.com/non-commercial-datasets/) et [fichiers](https://datasets.imdbws.com/).

Échéance officielle : dimanche 15 novembre 2026, 23 h 59, sans fuseau précisé.
Les prolongations doivent être demandées **et approuvées** avant le vendredi 13.
L'envoi final à `joe@adaltas.com`, avec l'autre membre en copie, sera effectué par
les étudiants une fois le projet terminé. Le README racine contient déjà les
noms, identifiants GitHub et le groupe.

## Installation native — chemin testé sous Linux cloud

Outils testés : Python 3.12.14, OpenJDK 21.0.13, PySpark 4.2.0, py4j 0.10.9.9,
pytest 9.0.2, Git 2.52.0. Spark 4.2.0 correspond à la version enregistrée dans le
notebook exécuté du TP05. Les anciens TP ne sont pas modifiés.

Il faut un Python compatible avec PySpark 4.2, un JDK 17 ou ultérieur et un accès
HTTPS à GitHub, PyPI et `datasets.imdbws.com`. Le lancement natif Windows n'a pas
été testé ; utiliser le chemin Docker ci-dessous pour éviter la configuration
Hadoop/Java native de Windows.

Depuis un terminal Linux, dans le dossier parent du dépôt :

```bash
git clone --branch feat/imdb-data-preparation https://github.com/benjaminlabos/ece-big-data-processing.git
cd ece-big-data-processing
git status --short --branch
python -m venv .venv
.venv/bin/python -m pip install -r requirements-preparation.txt
.venv/bin/python -m pytest -q tests
```

Les commandes suivantes s'exécutent toutes à la racine du dépôt. Les noms des
dossiers d'entrée et de sortie doivent être libres pour une nouvelle exécution.

### Jeu contrôlé reproductible — aucune donnée IMDb

```bash
.venv/bin/python -m imdb_preparation.fixture --destination data/raw/fixture-v1
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/fixture-v1 --output results/fixture-v1 --as-of-year 2026
```

Ce jeu couvre les sept tables : doublons, clés absentes/invalides/conflictuelles,
valeurs nulles, `\N`, champs vides ou blancs, nombres invalides, décès avant
naissance, fin avant début et références absentes. Une année de sortie future
reste autorisée ; une naissance ou un décès futur est invalidé par `--as-of-year`.
La saison 0 reste autorisée pour les épisodes spéciaux.

### Échantillon réel IMDb — premières 1 000 lignes de chaque table

```bash
.venv/bin/python -m imdb_preparation.download --destination data/raw/imdb-sample-2026-11-10 --rows 1000
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/imdb-sample-2026-11-10 --output results/sample-2026-11-10 --as-of-year 2026
```

Le téléchargement conserve les octets des lignes source et écrit un manifeste
avec URL, date, ETag/Last-Modified si disponibles, taille et SHA256 de chaque
fichier. Les fichiers limités sont des TSV ; les fichiers complets restent gzip.
IMDb est actualisé quotidiennement : un téléchargement ultérieur peut produire
des résultats différents. Les sept téléchargements ne forment pas une transaction
atomique côté IMDb ; les en-têtes HTTP sont conservés pour tracer leur version.

Un échantillon de tête est biaisé et ses tables parentes sont incomplètes.
Les références non trouvées y sont donc conservées avec l'action
`retained_unverified` et la règle `reference_not_in_snapshot` ; ces nombres ne
doivent pas être présentés comme des taux d'orphelins du jeu complet. Les autres
contrôles et corrections s'appliquent normalement.

### Données complètes — commande prévue, exécution intégrale à vérifier

```bash
.venv/bin/python -m imdb_preparation.download --destination data/raw/imdb-full-v1
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/imdb-full-v1 --output results/full-v1 --shuffle-partitions 32 --as-of-year 2026
```

L'absence de `--rows` demande les sept fichiers complets. Le mode complet vérifie
les références contre les clés des parents nettoyés et applique les corrections.
Une référence peut être absente parce que la ligne parente a été mise en
quarantaine ; la règle s'appelle donc `reference_not_in_clean_parent`.
Ne pas transformer manuellement le manifeste d'un échantillon en manifeste
complet. Le même code traite toutes les tailles ; ajuster les ressources et
`--shuffle-partitions`, sans réécrire les transformations.

Les TSV gzip ne sont pas divisibles en partitions d'entrée par Spark. Le pipeline
utilise des caches Spark sur disque, des jointures et des sorties Parquet :
prévoir de l'espace pour les bruts, les caches, les sorties et les quarantaines,
au-delà de la taille des fichiers compressés. Les essais enregistrés ici portent
sur le jeu contrôlé et un échantillon réel, pas sur IMDb complet. Le dimensionnement
et la durée de l'exécution intégrale restent à mesurer.

## Windows PowerShell avec Docker — chemin validé

Prérequis : Docker Desktop actif, moteur Linux, Compose v2, dépôt cloné.
Le service réutilise exactement l'image Spark épinglée du TP05. Il ne démarre ni
Kafka ni Jupyter. La préparation ne dépend pas du travail streaming de Benjamin.
Il expose les chemins Python Spark/py4j comme le service du TP05 et désactive le
contrôle de santé Jupyter hérité de l'image, inutile pour un traitement batch.

Depuis `C:\Users\nriha\ece-big-data-processing` :

```powershell
git status --short --branch
docker info
docker compose -f compose.preparation.yaml config -q
docker compose -f compose.preparation.yaml pull preparation
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.fixture --destination data/raw/fixture-win-v1
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.prepare --raw-dir data/raw/fixture-win-v1 --output results/fixture-win-v1 --as-of-year 2026
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.download --destination data/raw/imdb-sample-win-v1 --rows 1000
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.prepare --raw-dir data/raw/imdb-sample-win-v1 --output results/sample-win-v1 --as-of-year 2026
```

Résultat attendu : sept tables annoncées, un fichier `SUCCESS.json` dans chaque
dossier final, ainsi que les sorties décrites ci-dessous. Les chemins `data/` et
`results/` sont montés depuis le dépôt Windows et restent disponibles après la
fermeture du conteneur. Le jeu contrôlé a été exécuté avec ces commandes sur le
PC Windows le 10 novembre : Docker 28.5.1, PySpark 4.2.0, Python 3.13.15 et Java
21.0.12. Le téléchargement et le traitement de l'échantillon réel ont également
réussi. Les compteurs et le profil manquant correspondent à ceux du test cloud.
Un premier essai renvoyait `Docker Desktop is unable to start` ; le redémarrage
manuel de Docker Desktop par Nicolas a rétabli l'accès au daemon. En cas de même
message, vérifier d'abord que `docker info` fonctionne avant de lancer les jobs.
Pour une erreur de droits sur un hôte Linux, donner à l'utilisateur du conteneur
un accès en écriture aux dossiers de données et résultats du montage.

## Sorties et règles techniques appliquées

Pour une exécution réussie, sous le chemin donné à `--output` :

| Chemin | Contenu |
| --- | --- |
| `SUCCESS.json` | Marqueur final avec périmètre et date |
| `input_manifest.json` | Copie du manifeste source et de ses empreintes |
| `missing_profile.json` | 39 couples table/colonne, nombres de lignes, valeurs manquantes et pourcentages |
| `top5_missing.json` | Les cinq colonnes au pourcentage manquant le plus élevé |
| `summary.json` | Versions, périmètre, année de contrôle, compteurs et anomalies par table |
| `clean/<table>/` | Dossier Parquet Spark, identifiants string, nombres bigint/double, listes array<string> |
| `anomalies/<table>/<étape>/` | Parquet : table, clé JSON, colonne, règle, ancienne valeur, action |
| `quarantine/<table>/<étape>/` | Parquet : table, clé JSON, ligne JSON et raison |

Les noms de tables sont ceux d'IMDb, par exemple `title.basics` et `name.basics`.
Il faut lire un **dossier** Parquet entier, pas un seul fichier `part-*`.
`characters` reste une chaîne JSON source ; les listes IMDb sont converties en
tableaux (virgule, ou caractère U+0002 pour `title.akas.types/attributes`).
La colonne `title.akas.language` est la langue du titre alternatif et ne constitue
pas une preuve de la langue originale du film.

Le profil est calculé sur les colonnes source lues comme chaînes, **avant** toute
suppression ou conversion. Une cellule est manquante si elle est nulle, vide,
blanche, ou égale à `\N` après suppression des espaces périphériques. Chaque ligne
source, doublons compris, compte dans le dénominateur de sa table. Une table vide
donne un pourcentage JSON `null`, pas zéro. Le top 5 départage les égalités par
table puis colonne et exclut les colonnes sans dénominateur.

Le nettoyage normalise les champs manquants en null et enlève les espaces
périphériques. Il fusionne les lignes identiques après cette normalisation et
compte leur nombre dans `normalized_duplicate_rows_removed`. Il ne choisit pas
arbitrairement entre des lignes différentes ayant la même clé : elles sont toutes
mises en quarantaine. `ordering` est normalisé numériquement avant le contrôle
d'unicité, donc `01` et `1` représentent la même clé. Les clés composites sont
`(titleId, ordering)` pour les titres alternatifs et `(tconst, ordering)` pour les
principaux participants ; les autres clés sont `tconst` ou `nconst`.

Les nombres mal formés, hors domaine ou les années incohérentes deviennent null
avec une trace d'anomalie. Les années valides sont entre 1 et 9999 ; pour les
personnes, elles ne dépassent pas `--as-of-year`. Les notes sont entre 1 et 10,
les durées et `ordering` sont strictement positifs, les votes/saisons/épisodes
non négatifs, les indicateurs booléens valent 0 ou 1. Si décès < naissance ou
fin < début, seule la seconde valeur est mise à null ; il n'y a pas d'imputation.
Les éléments vides et doublons de listes sont retirés avec une trace.

Les références contrôlées couvrent les titres alternatifs, notes, épisodes et
séries parentes, principaux participants, réalisateurs, auteurs et
`knownForTitles`. En mode complet/contrôlé, une référence absente est retirée
d'une liste ou mise à null ; si elle constitue la clé du titre enfant, la ligne
entière est mise en quarantaine. Les jointures utilisent des clés parentes
distinctes pour éviter de multiplier les lignes.

Pour chaque table, l'exécution vérifie l'égalité :

```text
raw_rows = clean_rows + quarantined_rows + normalized_duplicate_rows_removed
```

Les compteurs d'anomalies portent sur des occurrences par colonne/règle, pas sur
des lignes distinctes ; plusieurs corrections peuvent concerner une même ligne.
Les données source restent séparées et inchangées. Les quarantaines contiennent
la ligne normalisée avant exclusion ; les octets originaux sont dans les bruts.

### Consulter les résultats

Exemple Linux pour lire le profil et les compteurs sans dépendance supplémentaire :

```bash
.venv/bin/python -m json.tool results/sample-2026-11-10/top5_missing.json
.venv/bin/python -m json.tool results/sample-2026-11-10/summary.json
```

Depuis une session PySpark, avec `spark` déjà créé :

```python
spark.read.parquet("results/sample-2026-11-10/clean/title.basics").show(10, truncate=False)
spark.read.option("recursiveFileLookup", True).parquet("results/sample-2026-11-10/anomalies/name.basics").show(10, truncate=False)
```

## Relances et échecs

Le téléchargement exige un nouveau dossier : il ne mélange pas les générations
quotidiennes. Il écrit les fichiers via `.part`, puis le manifeste seulement quand
les sept fichiers sont terminés. Un dossier sans manifeste est incomplet ; garder
ses fichiers pour diagnostiquer puis choisir un nouveau nom pour relancer.
La préparation vérifie tous les SHA256 avant la lecture Spark et refuse les chemins
entrée/sortie imbriqués ainsi que l'écrasement d'un résultat existant.

Le résultat est d'abord écrit dans `<output>.incomplete-<id>`, puis renommé en
`<output>` uniquement si tout réussit. Après un échec, les fichiers intermédiaires
restent disponibles pour diagnostic. On peut relancer vers le même `<output>`
final absent ; l'ancien dossier `.incomplete-*` ne sera pas repris. Pour réutiliser
des résultats complets existants, conserver les bruts et choisir un nouveau nom
de sortie. Le TSV doit respecter exactement l'en-tête et le nombre de colonnes ;
les erreurs de parsing font échouer le traitement, sans ignorer de lignes.

## Validation exécutée

Vérifications réalisées le 10 novembre 2026 dans le cloud Linux, puis sur le PC
Windows avec Docker Desktop :

- Installation native, lancement des modules `fixture`, `download` et `prepare`.
- `.venv/bin/python -m pytest -q tests` : **7 tests réussis** (39,24 s lors du
  dernier passage). Les tests vérifient tous les dénominateurs, les 39 colonnes,
  le classement global, les types, les dates, les clés et références, les chaînes
  contenant des guillemets, les tables vides, les empreintes, les relances et le
  refus d'un TSV mal formé sans publication de sortie finale.
- Jeu contrôlé de **32 lignes** : **19 lignes nettoyées**, **12 en quarantaine**,
  **1 doublon normalisé retiré**. Les corrections sont vérifiées sur les valeurs
  attendues, pas seulement sur les compteurs.
- Échantillon IMDb réel de **1 000 lignes par table**, soit **7 000 lignes** :
  sept tables Parquet, profil des 39 colonnes, top 5, audit et réconciliation des
  lignes produits. Aucune ligne mise en quarantaine sur cet extrait. Les références
  absentes des extraits parents sont signalées et conservées.
- `ruff 0.14.3` : `ruff check imdb_preparation tests` et
  `ruff format --check imdb_preparation tests` réussis (outil de développement
  facultatif, installé séparément).
- `git diff --check`, exclusion Git des données/caches et validation
  `docker compose -f compose.preparation.yaml config -q` réussies.
- Docker cloud : daemon 29.9.2 accessible. Sur le PC Windows, daemon 28.5.1
  accessible après redémarrage de Docker Desktop ; configuration Compose valide.
  Le job contrôlé conteneurisé a traité les sept tables en environ 151 secondes
  hors démarrage/arrêt de Spark, avec les mêmes compteurs que le test cloud :
  32 lignes brutes, 19 nettoyées, 12 en quarantaine et 1 doublon retiré.
  Le téléchargement et le traitement de 7 000 lignes IMDb réelles ont également
  réussi sur ce PC, en environ 145 secondes de traitement hors démarrage/arrêt de
  Spark. Les sept empreintes source et les 39 profils manquants correspondent à
  l'exécution cloud. Preuve : `validation/windows-docker-2026-11-10.json`.

Les preuves JSON agrégées et les empreintes des entrées se trouvent dans
`validation/`. Elles sont distinctes du rapport et n'incluent aucun fichier IMDb
brut ni sortie Parquet. Le classement de l'échantillon réel n'est pas celui du
jeu complet : `title.basics.endYear` 100 %, `title.akas.attributes` 91,9 %,
`title.akas.language` 91,7 %, `title.principals.job` 80,8 % et
`title.principals.characters` 79 %.

## Interface proposée à Benjamin — en attente d'accord

Proposition : consommer les dossiers Parquet `clean/title.basics`,
`clean/name.basics`, `clean/title.principals` et `clean/title.akas`, en vérifiant le
`scope` dans `summary.json` et la présence de `SUCCESS.json`. Les identifiants
`tconst`/`nconst` restent des chaînes IMDb ; `genres`, `directors`, `writers`,
`knownForTitles` sont des tableaux de chaînes. Les noms de colonnes IMDb sont
conservés. Aucun identifiant Wikipédia n'est inventé par cette préparation.
Le rapprochement IMDb↔Wikipédia relève de sa branche streaming/enrichissement.

Pour limiter les conflits de documentation, Nicolas tient cette première section
préparation ; Benjamin pourra ajouter des sections streaming et enrichissement à
la fin du fichier, en réactualisant sa branche depuis `main` après fusion approuvée.
Cette organisation doit également être confirmée avec lui.

## Déclaration d'usage d'IA

OpenAI Codex (assistant de code) a été utilisé par Nicolas pour inspecter le dépôt
et les sources officielles, configurer l'environnement de préparation, développer
les modules PySpark et le téléchargement, écrire/exécuter les tests et rédiger ce
guide d'installation/exploitation ainsi que le descriptif technique de PR.
Les résultats enregistrés proviennent d'exécutions du code. Aucun texte de
`REPORT.md`, synopsis ou biographie n'a été rédigé ou généré dans cette tâche.
Les étudiants doivent compléter cette déclaration si d'autres outils ou tâches
utilisent l'IA ; elle ne préjuge pas du travail réalisé séparément par Benjamin.

## Git et relecture

Branche : `feat/imdb-data-preparation`. Les commits de cette tâche sont attribués
à Nicolas Rihana, avec l'assistance Codex déclarée ci-dessus. La PR doit être
relue et approuvée par Benjamin avant fusion ; aucun changement direct sur `main`.
Les bruts, résultats, environnements, caches et secrets sont exclus par `.gitignore`.
La validation et le descriptif de PR permettent la relecture sans données brutes
dans Git. Vérifier les fichiers indexés avant chaque commit.

Le 10 novembre, le commit de préparation a été transféré par bundle Git vérifié
vers le dépôt Windows propre, puis poussé depuis le PowerShell du PC. Le push via
le jeton de l'environnement cloud renvoyait 403, tandis que l'authentification
Git locale Windows a fonctionné. La [PR n° 1](https://github.com/benjaminlabos/ece-big-data-processing/pull/1)
est ouverte vers `main`, avec Benjamin demandé en relecture.
