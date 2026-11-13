# Big Data Processing — gr-04

Navigation : [préparation et exploration](PROJECT.md), [streaming Wikipédia/Kafka/Spark](PROJECT_STREAMING.md).

Guide technique de lancement. Les explications, choix méthodologiques et
interprétations du rapport sont à écrire par les étudiants dans `REPORT.md`.
Ce guide couvre la préparation IMDb, les interfaces d’intégration, l’exploration
et la sélection des films. Les pipelines de Benjamin sont développés dans leurs
PR respectives ; ce guide ne décrit pas encore une exécution fusionnée complète.

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
Pour les fichiers complets, la taille reçue doit correspondre au `Content-Length`
lorsqu'il est fourni ; une lecture gzip jusqu'à EOF vérifie aussi les en-têtes,
les sommes CRC et les fins de membres avant publication du fichier. Un échec
conserve le `.part` et ne produit pas de manifeste final. Cette vérification ne
requiert pas d'écrire une copie décompressée sur disque. Les réponses HTTP autres
que 200 sont refusées.
Le manifeste indique `source_compressed_content_length` et
`gzip_integrity_verified`. Pour un échantillon, on ne demande qu'un préfixe de
lignes : la taille du TSV n'est donc pas comparée à celle du gzip source, et
`gzip_integrity_verified` vaut `false`.
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
blanche, ou égale à `\N` après suppression des espaces périphériques Unicode,
y compris les espaces insécables U+00A0/U+202F. Les espaces internes d'un texte
non vide sont préservés. Chaque ligne
source, doublons compris, compte dans le dénominateur de sa table. Une table vide
donne un pourcentage JSON `null`, pas zéro. Le top 5 départage les égalités par
table puis colonne et exclut les colonnes sans dénominateur.

Le nettoyage normalise les champs manquants en null et enlève les espaces
périphériques. Il fusionne les lignes identiques après cette normalisation et
compte leur nombre dans `normalized_duplicate_rows_removed`. Il ne choisit pas
arbitrairement entre des lignes différentes ayant la même clé : elles sont toutes
mises en quarantaine. Un `ordering` valide est normalisé numériquement **avant
le dédoublonnage des lignes**, puis avant le contrôle d'unicité : deux lignes
identiques sauf `01`/`1` deviennent une ligne, tandis que deux contenus différents
sur cette clé restent un conflit. Les valeurs invalides gardent leur texte pour
la quarantaine. Les clés composites sont
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

## Validation de la première version (376b434)

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

## Interfaces IMDb convenues avec Benjamin

Les identifiants nettoyés `tconst`/`nconst` ont été acceptés pour filtrer son
catalogue Wikipédia, et le CSV `tconst,nconst` doit contenir un couple distinct
et non nul par ligne. Les dossiers Parquet disponibles sont `clean/title.basics`,
`clean/name.basics`, `clean/title.principals`, `clean/title.akas` et
`clean/title.ratings`, en vérifiant le
`scope` dans `summary.json` et la présence de `SUCCESS.json`. Les identifiants
`tconst`/`nconst` restent des chaînes IMDb ; `genres`, `directors`, `writers`,
`knownForTitles` sont des tableaux de chaînes. Les noms de colonnes IMDb sont
conservés. Aucun identifiant Wikipédia n'est inventé par cette préparation.
Le rapprochement IMDb↔Wikipédia relève de sa branche streaming/enrichissement.
`clean/title.ratings` fournit `tconst` (string), `averageRating` (double) et
`numVotes` (bigint) ; il est déjà écrit par la préparation et les exports peuvent
le joindre au catalogue de films. Le seuil de durée, le minimum de votes et les
catégories de participants pour la sélection définitive restent à convenir entre
les étudiants. Aucune valeur n'a été décidée par l'assistant.

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

## Corrections après la relecture de Benjamin — PR n° 1

Les trois cas signalés ont été reproduits avant correction : acceptation d'un
transfert HTTP tronqué, faux conflits entre lignes identiques avec `ordering=01`
et `ordering=1`, et omission des cellules composées d'espaces insécables dans le
profil. Six cas paramétrés échouaient sur le commit relu.

Le téléchargement complet exige maintenant HTTP 200, vérifie le `Content-Length`
quand il est annoncé, puis lit tout le gzip pour valider sa structure et ses CRC.
Un transfert tronqué, un gzip incomplet/corrompu ou une réponse partielle 206 ne
peut pas produire de manifeste final. Les `.part` restent disponibles. Les tests
de transport utilisent des réponses contrôlées, avec et sans taille HTTP ; ils
ne représentent pas un téléchargement du lot IMDb complet.

La normalisation d'un `ordering` valide précède le dédoublonnage. Les lignes qui
ne diffèrent que par `01`/`1` sont fusionnées, mais les vrais conflits restent en
quarantaine. Les valeurs invalides ne sont pas converties en null avant cette
quarantaine. Le profil et la normalisation utilisent les espaces Unicode, y
compris U+00A0, U+202F et U+2007, également aux bords des éléments de listes.

La suite de préparation compte désormais **21 tests réussis** sous Spark 4.2.0,
dont 10 tests de transport et 4 nouveaux cas de qualité. Le jeu contrôlé conserve
les mêmes 32 lignes brutes et donne maintenant **20 lignes nettoyées, 10 en
quarantaine et 2 doublons retirés**. Le changement provient du faux conflit
`01`/`1` corrigé. Les fichiers `validation/fixture-v1.json` et
`validation/windows-docker-2026-11-10.json` conservent les résultats historiques
avant correction ; la nouvelle preuve est
`validation/preparation-review-fixes-2026-11-10.json`.

Les **21 tests ont également réussi dans l'image Docker sur le PC Windows**
(112,94 s de tests, Spark 4.2.0/Python 3.13.15). `pytest 9.0.2`, absent de l'image,
a été installé dans le compte utilisateur du conteneur éphémère, sans modifier
le Python de l'hôte. Commande à la racine du dépôt :

```powershell
docker compose -f compose.preparation.yaml run --rm preparation bash -c 'python -m pip install --user pytest==9.0.2 && python -m pytest -q tests'
```

Pour relancer les bruts existants sans écraser leurs anciens résultats :

```bash
.venv/bin/python -m pytest -q tests
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/fixture-v1 --output results/fixture-review-v2 --as-of-year 2026
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/imdb-sample-2026-11-10 --output results/sample-review-v2 --as-of-year 2026
```

Le lot IMDb complet reste à valider. Les explications et justifications du rapport
restent à rédiger par les étudiants. La PR attend une nouvelle relecture de
Benjamin sur les corrections ; aucune approbation de pair n'est remplacée par
les tests automatiques.

## Exports d'intégration — branche `feat/imdb-integration-exports`

Cette tâche dépend de la préparation de la PR n° 1. Sa PR vise la branche de
préparation pour isoler les changements ; la cible pourra devenir `main` une fois
la préparation fusionnée après approbation. Benjamin a confirmé le filtrage par
identifiants nettoyés, les couples distincts non nuls du CSV et l'ajout de
`clean/title.ratings` à l'interface. Les seuils et catégories du lot définitif
restent à convenir.

Le module `imdb_preparation.exports` lit uniquement un résultat préparé contenant
`SUCCESS.json`, son `summary.json` cohérent et les tables Parquet attendues.
Il vérifie les types et l'unicité des clés parentes avant les jointures.
Les transformations, filtres, dédoublonnages et jointures sont réalisés en Spark.

### Sorties

| Chemin sous `--output` | Contenu |
| --- | --- |
| `imdb_ids/` | Parquet `imdb_id,entity_type` : tous les titres et personnes nettoyés |
| `movie_catalog/` | Parquet des `titleType=movie` sortis de 1980 à 2025, avec genres, durée, note et votes ; films sans note conservés |
| `candidates.csv` | Si sélection fournie : fichier UTF-8 unique, en-tête exact `tconst,nconst`, un couple distinct par film/personne |
| `enrichment_coverage/` | Parquet : statut de chaque titre demandé, nombres de crédits et personnes disponibles, indicateur de moins de trois candidats |
| `rejected_credits/` | Crédits sans identifiant de personne ou absents de `name.basics` nettoyée |
| `streaming_reference.csv` | Si référence fournie : fichier UTF-8 unique `wiki,title,imdb_id,wikidata_id`, filtré par les identifiants nettoyés |
| `rejected_reference/` | Références mal formées ou identifiants absents du résultat préparé |
| `summary.json`, `SUCCESS.json` | Périmètre hérité (`fixture`, `sample`, `full`), empreintes des entrées, compteurs et marqueur final |

Le catalogue de films n'applique ni classement, ni seuil de votes, ni seuil de
durée. Il prépare les données pour vos choix ultérieurs. La sélection pour
l'enrichissement vient d'un CSV externe contenant **uniquement** la colonne
`tconst`. Les doublons y sont normalisés et comptés. Chaque titre distinct demandé
reste dans la couverture, même s'il est invalide, absent, hors période ou d'un
autre type qu'un film. L'export de crédits retient tous les participants principaux
présents dans les noms nettoyés ; une personne cumulant plusieurs rôles ne crée
qu'un couple. Aucun participant n'est inventé pour atteindre trois personnes.

Un indicateur de moins de trois candidats signale un manque à traiter ; même
trois candidats ne garantissent pas trois biographies disponibles chez la source.
Un film sans crédit exportable reste dans `enrichment_coverage`, mais ne peut pas
être transmis par le CSV à deux colonnes actuel. Il faudra en tenir compte lors
du branchement du collecteur. `consumer_ready` signifie seulement que le CSV
concerné contient au moins une ligne.

Le streaming conserve les identifiants de toutes les tables parentes, sans filtre
1980–2025 ou sélection de films. Il utilise le catalogue Wikipédia **fourni** par
Benjamin : aucune nouvelle requête Wikidata ou Wikipédia n'est faite ici. Les
titres, accents, casse et identifiants Q sont conservés. Les correspondances
écartées d'un échantillon restent non vérifiées, et non des erreurs établies d'IMDb.

Les deux CSV sont des fichiers, et non des dossiers `part-*`, pour être lisibles
par les scripts de Benjamin. Spark écrit une partition triée, déplacée en fichier
sans collecte des lignes dans le pilote Python. Cette consolidation ne concerne
pas les grands catalogues Parquet distribués. Les CSV vides contiennent l'en-tête ;
consulter les compteurs avant de lancer un consommateur qui exige des lignes.

Les réconciliations vérifiées sont :

```text
crédits des films retenus = couples candidats + doublons de couples retirés + crédits rejetés
références d'entrée = liens exportés + doublons de liens retirés + références rejetées
```

Les bruts et résultats préparés restent inchangés. Comme pour la préparation,
l'export refuse l'écrasement et les chemins imbriqués avec son entrée préparée,
écrit dans un dossier `.incomplete-*` puis renomme le résultat seulement si tout
réussit. Une relance utilise un nouveau nom de sortie ; un échec ne publie pas de
dossier final.

### Lancement natif Linux testé

Après les étapes précédentes de préparation, à la racine du dépôt :

```bash
.venv/bin/python -m pytest -q tests
.venv/bin/python -m imdb_preparation.exports --prepared-dir results/fixture-v1 --output results/integration-fixture-v1 --selected-titles examples/integration/fixture-selected.csv --wiki-reference examples/integration/fixture-reference.csv
git fetch origin
mkdir -p data/reference
git show 4855223aa45a0af1cf9f1a582d3806083aa3a2f8:05.structured-streaming/lab_docker_wikistreams/jobs/pilot_reference.csv > data/reference/benjamin-pilot.csv
.venv/bin/python -m imdb_preparation.exports --prepared-dir results/sample-2026-11-10 --output results/integration-sample-v1 --wiki-reference data/reference/benjamin-pilot.csv
```

Le répertoire du résultat réel du dernier essai cloud est
`results/sample-2026-11-10-final` ; la commande ci-dessus réutilise le nom de la
recette initiale depuis zéro. La référence pilote est figée au commit indiqué.
`--reference-source URL` peut enregistrer son URI de provenance dans le résumé.
Pour l'enrichissement réel, ajouter `--selected-titles chemin/selection.csv`
uniquement une fois votre sélection préparée ; le module n'en choisit pas une.

Les fichiers `examples/integration/fixture-*.csv` sont **synthétiques** et servent
uniquement avec le jeu contrôlé. Leurs correspondances Wikipédia ne doivent pas
alimenter une collecte ou une capture réelle.

### Lancement Docker/Windows

Depuis `C:\Users\nriha\ece-big-data-processing`, après la préparation Docker
décrite plus haut :

```powershell
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.exports --prepared-dir results/fixture-win-v1 --output results/integration-fixture-win-v1 --selected-titles examples/integration/fixture-selected.csv --wiki-reference examples/integration/fixture-reference.csv
git fetch origin
New-Item -ItemType Directory -Force data/reference | Out-Null
cmd /d /c "git show 4855223aa45a0af1cf9f1a582d3806083aa3a2f8:05.structured-streaming/lab_docker_wikistreams/jobs/pilot_reference.csv > data\reference\benjamin-pilot.csv"
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.exports --prepared-dir results/sample-win-v1 --output results/integration-sample-win-v1 --wiki-reference data/reference/benjamin-pilot.csv
```

La redirection par `cmd` préserve les octets UTF-8 de la référence Git, y compris
les accents, avec Windows PowerShell 5.1. Les deux commandes d'export ci-dessus
ont été exécutées avec succès sur le PC Windows sous Docker 28.5.1/Spark 4.2.0.
Leurs compteurs correspondent au cloud : 8 identifiants et 2 liens pour le jeu
contrôlé, puis 2 000 identifiants et 14 liens sur l'échantillon réel. Le second
lancement n'a pas reçu de sélection de films et n'a donc pas créé de CSV de crédits.
Preuve : `validation/integration-exports-windows-2026-11-10.json`.

### Validation des exports et limites actuelles

La suite complète passe maintenant avec **27 tests** : les 21 tests de préparation
après relecture et les 6 tests d'exports. Les tests d'exports couvrent notamment
les films sans note, les bornes
1980–2025, les sélections invalides, les clés parentes dupliquées, les multiples
rôles d'une même personne, les personnes absentes, les CSV vides, les accents,
guillemets et les réconciliations. Ruff et le contrôle d'espaces Git passent.

Le lancement contrôlé exporte 8 identifiants nettoyés, 1 film, 1 couple candidat et
2 liens Wikipédia synthétiques sur 3. La couverture signale qu'il manque des
candidats pour atteindre trois personnes sur ce film.

L'essai réel exporte **2 000 identifiants** et **14 des 32 liens** du catalogue
pilote de Benjamin. Les 18 autres sont signalés comme absents de cet échantillon.
Les premières 1 000 lignes de `title.basics` ne contiennent aucun film de
1980–2025. *Inception* et *The Dark Knight*, sélection pilote de Benjamin testée
séparément, sont tous deux signalés `not_in_prepared_titles`, avec zéro couple
exportable. Il faut un périmètre IMDb plus large pour alimenter réellement leur
enrichissement. Ce contrôle n'est pas un classement top 10 ni une validation
sur IMDb complet.

Les deux CSV ont été relus sous Spark 4.2.0 avec le lecteur de référence exact de
la PR streaming (commit `4855223`) et le schéma candidat de la PR enrichissement
(commit `9754537`). Cela valide les interfaces de lecture ; aucune nouvelle
capture de 15 minutes ou collecte Wikipédia n'a été effectuée par cette tâche.
Preuves agrégées : `validation/integration-exports-2026-11-10.json` et
`validation/integration-consumer-compatibility.json`.

Les corrections de la PR n° 1 ont été réintégrées dans cette branche : le conflit
de documentation a été résolu en conservant les deux sections, et les 27 tests
ont été relancés sous Spark 4.2.0. Les preuves d'exports originales restent celles
d'avant relecture ; la preuve actuelle est
`validation/integration-after-review-2026-11-10.json`.

### Contrôle Arrow de la fonction d'enrichissement de Benjamin

Le contrôle a réussi dans l'image Docker Spark 4.2.0 du projet, avec pandas 2.3.3,
PyArrow 25.0.0 et BeautifulSoup 4.14.3 (version demandée par son pilote). Il charge
la fonction `extract_text` exacte du commit d'enrichissement `4eea937`, active les
UDF Python avec Arrow et vérifie le type d'évaluation 101 et le plan
`ArrowEvalPython`. Les résultats des trois lignes synthétiques — synopsis de test,
début biographique de test et HTML absent — correspondent aux valeurs attendues.
La conversion `toPandas()` est également vérifiée avec le repli Arrow désactivé.

Cela vérifie la compatibilité d'exécution de cette fonction et du transfert Arrow.
Ce n'est pas une nouvelle exécution du pilote réel à deux films ni une validation
complète de la collecte, des jointures ou du lot final. Aucun texte de source
réelle n'est collecté ni remplacé. La preuve est
`validation/enrichment-arrow-review-2026-11-10.json`.

Reproduction depuis PowerShell à la racine du dépôt, avec un nouveau chemin de
sortie si le précédent existe déjà :

```powershell
git fetch origin
New-Item -ItemType Directory -Force data/review-checks | Out-Null
cmd /d /c "git show 4eea937842418fdfe45b22d845c997ee23a06abb:project/enrichment/enrich.py > data\review-checks\enrich.py"
cmd /d /c "git show 4eea937842418fdfe45b22d845c997ee23a06abb:project/enrichment/requirements.txt > data\review-checks\requirements.txt"
docker compose -f compose.preparation.yaml run --rm preparation bash -c 'python -m pip install --user -r data/review-checks/requirements.txt && python validation/check_enrichment_arrow.py --enrichment-module data/review-checks/enrich.py --source-commit 4eea937842418fdfe45b22d845c997ee23a06abb --output results/enrichment-arrow-check-v1.json'
```

Codex a assisté Nicolas pour vérifier les contrats des PR de Benjamin, développer
ces exports Spark, écrire/exécuter leurs tests et documenter cette interface.
Aucun texte n'a été ajouté à `REPORT.md`. Le classement et ses justifications
restent à définir et rédiger par les étudiants.

## Exécution IMDb complète et exploration — branche `feat/imdb-full-analysis`

Le 11 novembre, les sept fichiers complets ont été téléchargés sur le PC Windows :
2 003 620 065 octets compressés, tous vérifiés par taille HTTP quand disponible
et intégrité gzip. Ce constat valide le téléchargement, pas encore le nettoyage
du lot complet. Le manifeste versionné dans `validation/full-download-2026-11-11.json`
permet d'identifier ces entrées ; les fichiers restent hors Git.

Le nettoyage évite désormais de conserver simultanément les caches bruts,
dédoublonnés, valides et typés. Chaque cache précédent est libéré après
matérialisation du suivant. `progress.json` est publié atomiquement après chaque
table dans le dossier `.incomplete-*`. Il reste distinct de `SUCCESS.json`.

`compose.full.yaml` donne 3 GiB au pilote Spark, limite le conteneur à 6 GiB et
place les fichiers temporaires dans son disque Linux. Le lanceur surveille le
disque hôte et arrête uniquement les conteneurs nommés de cette tâche sous 5 GiB
libres. Les données brutes et sorties partielles restent conservées.

Depuis la racine du dépôt dans PowerShell, Docker Desktop actif, sur une nouvelle
installation où ces chemins ne sont pas encore présents :

```powershell
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.download --destination data/raw/imdb-full-2026-11-11
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.languages --output data/reference/original-languages-2026-11-11.csv
.\run_full_imdb.ps1
```

Si les entrées existent déjà avec leurs manifestes, les conserver et lancer
uniquement le script. Pour une nouvelle exécution avec sorties existantes,
fournir de nouveaux chemins via `-PreparedOutput` et `-AnalysisOutput`.

Ce lanceur enchaîne préparation complète (4 threads, 64 partitions de mélange),
exploration provisoire, puis export des crédits des films sélectionnés. Logs :
`results/imdb-full-prepare.log`, `results/imdb-full-exploration.log` et
`results/imdb-full-credit-export.log`. Choisir de nouveaux noms de sorties pour
relancer après un échec ; aucune reprise de cache Spark n'est promise.

L'exploration est aussi exécutable séparément :

```powershell
docker compose -f compose.preparation.yaml -f compose.full.yaml run --rm preparation python -m imdb_preparation.explore --prepared-dir results/imdb-full-2026-11-11 --output results/exploration-full-provisional-2026-11-11 --config examples/exploration/provisional.json
```

Le module fournit `q1` à `q8` en Parquet, un classement `top10_per_genre_year`,
un fichier `selected_titles.csv` dédupliqué et un résumé avec périmètre, définitions,
paramètres et statuts. Le tri top 10 est note décroissante, votes décroissants,
puis identifiant croissant. Les genres multiples comptent pour chaque genre ;
les comparaisons de groupes portent sur ces appartenances et ne représentent
pas des nombres de films disjoints. Les carrières sont celles observables dans
les films candidats, sans inférer une carrière complète hors IMDb.

Les seuils du fichier `provisional.json` sont des **valeurs de travail non validées
par les étudiants**. Tous les résultats restent marqués provisoires tant que
`student_validated` vaut `false`. Ils ne constituent aucune justification de
rapport. Le choix final des seuils et des catégories reste à Nicolas et Benjamin.

Q5 retient au moins trois saisons ayant chacune dix épisodes connus, exclut la
saison 0 et accepte les égalités de meilleures/pires notes de façon existentielle.
Q8 regroupe acteur/actrice en interprétation et écarte les apparitions `self` et
les archives. Ces définitions de travail sont conservées dans le code/résumé.
Les critères pourront être ajustés après lecture de la formulation officielle.

Q6 exige une source supplémentaire des langues **originales** : CSV
`tconst,language,source_url` à fournir via `--original-languages`. Sans cette
entrée, la question reste explicitement `blocked`, et aucun résultat de langue
n'est inventé depuis `title.akas`. Une entrée partielle ne permet pas de conclure
à l'exhaustivité des carrières multilingues.

Le collecteur dédié `imdb_preparation.languages` interroge la propriété Wikidata
[P364](https://www.wikidata.org/wiki/Property:P364), consacrée à la langue originale
du film ou de la série, avec le lien IMDb P345. Il ne collecte aucun synopsis ni
biographie et ne remplace pas la partie de Benjamin. Une seconde requête fournit
le vocabulaire ISO des langues (P218/P219/P220). La collecte conserve les octets
CSV et des manifestes SHA256 ; toutes les normalisations et jointures sont Spark.

```powershell
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.languages --output data/reference/original-languages-2026-11-11.csv
```

Le premier instantané recueilli contient 369 502 déclarations, avec une URI source
par ligne, sous CC0. Sa couverture n'est pas l'ensemble d'IMDb. Spark mesure la
part des films admissibles ayant une langue disponible. Il rejette les marqueurs
inconnus et choisit un code ISO stable par langue, afin que `en`/`eng` ne créent
pas deux langues. Les codes `zxx`, `und`, `mul` et `mis` n'identifient pas une langue
distincte et sont conservés dans les sorties non résolues plutôt que comptés.
Les références non résolues et les valeurs invalides restent auditables.

Si le CSV est présent au démarrage de la phase d'exploration du lanceur complet,
il est utilisé automatiquement ; sinon Q6 reste bloquée explicitement. La source
et le vocabulaire doivent correspondre à leurs empreintes dans le manifeste.

Le jeu contrôlé des tests couvre les huit requêtes et le rang maximal de dix
films par année/genre. Le lancement sur les petites données préparées reste un
contrôle du code ; les chiffres globaux ne seront annoncés qu'après la présence
des marqueurs finaux des exécutions complètes.

Codex assiste Nicolas pour l'optimisation des ressources, le lanceur, les calculs
paramétrables et leurs tests. Il ne modifie pas les branches streaming ou
enrichissement de Benjamin et ne rédige pas le rapport.

Le profil brut peut être calculé séparément, sur les mêmes sept fichiers et
empreintes, pendant le nettoyage. Il agrège les valeurs manquantes des 39 colonnes
sans tri ni cache et publie `PROFILE_SUCCESS.json`, jamais `SUCCESS.json` :

```powershell
docker compose -f compose.profile.yaml run --rm --name gr04-imdb-full-profile raw-profile python -m imdb_preparation.profile_full --raw-dir data/raw/imdb-full-2026-11-11 --output results/imdb-full-raw-profile-2026-11-11 --workers 4
```

Ce service dispose de 1 GiB de pilote et de 2 GiB de mémoire totale. Son succès
valide le profil des entrées ; il ne valide ni le nettoyage ni les jointures.
Les 33 tests contrôlés passent sous Spark 4.2.0 (63,01 s), y compris la
réconciliation exacte des 39 profils et la normalisation ISO. Un essai séparé
avec le premier instantané Wikidata réconcilie les 369 502 lignes : 364 117
énoncés valides mappés, 20 lignes invalides, 5 272 non résolues et 93 doublons
créés par les alias ISO fusionnés. Ce contrôle ne mesure pas encore la couverture
des films complets admissibles.

Chaque requête produit aussi un aperçu JSON limité à 20 lignes, extrait des
Parquet effectivement écrits, et un état `progress.json` actualisé après chaque
calcul. Le classement complet, avec titre, année, genre, rang, durée, note et
votes, est disponible en CSV dans `top10_per_genre_year.csv` ; le CSV dédupliqué
`selected_titles.csv` conserve l'interface attendue par l'export des crédits.

La population de Q1 comprend tous les films candidats dont la durée est connue,
y compris les films sans note. Le seuil de votes ne modifie pas la moyenne de
durée de leur genre ; il reste appliqué aux analyses portant sur les notes et
la popularité ainsi qu'au classement provisoire.

### Profil brut complet obtenu le 11 novembre

`validation/full-raw-profile-windows-2026-11-11.json` contient les 39 profils
calculés sur les sept fichiers complets, soit 216 477 904 lignes de tables,
avec le manifeste source et le top 5 global. Les pourcentages, dénominateurs,
ordre du top 5 et empreinte du module ont été contrôlés. L'empreinte du fichier
Python Windows porte sur ses octets CRLF ; Git et Linux conservent les fins LF.
Le marqueur `PROFILE_SUCCESS.json` est présent. Le nettoyage complet reste
suivi séparément : ce profil ne vaut pas validation de ses références.

Après réussite de la préparation, de l'exploration et de l'export des crédits :

```powershell
.\collect_full_results.ps1
```

Le collecteur refuse les sorties incomplètes ou hors périmètre complet. Il
compare les 39 comptes de valeurs manquantes au profil brut séparé, puis conserve
les résumés, neuf aperçus et empreintes des CSV dans
`validation/full-analysis-windows-2026-11-11.json`. Aucune interprétation de
rapport n'est générée. `-WaitForRunnerPid <PID>` permet d'attendre le lanceur
en cours ; `-Publish` ajoute un commit de cette seule preuve sur
`feat/imdb-full-analysis` et pousse cette branche sans fusionner les PR.

Dernière validation de la partie Python : 33 tests réussis sous Spark 4.2.0,
61,96 s, avec un film sans note inclus dans le contrôle des durées de Q1.

Pour calculer Q1–Q3 et le top 10 avant la fin des autres tables, le module
facultatif `imdb_preparation.explore_titles` réutilise la table des titres dont
`progress.json` certifie la fin, et nettoie séparément les notes avec les mêmes
fonctions de clés, valeurs et références. Il vérifie l'empreinte du fichier de
notes et le manifeste du profil complet. Les calculs sont partagés avec le module
d'exploration complet, sans seconde définition des requêtes.

```powershell
docker compose -f compose.analysis.yaml run --rm title-analysis python -m imdb_preparation.explore_titles --prepared results/imdb-full-2026-11-11 --snapshot data/raw/imdb-full-2026-11-11 --raw-profile results/imdb-full-raw-profile-2026-11-11 --output results/title-queries-full-provisional-2026-11-11 --config examples/exploration/provisional.json
```

Pendant le nettoyage, remplacer `--prepared` par le chemin `.incomplete-*`
imprimé par ce lancement et contenant la table des titres terminée. Le service
est limité à 1 152 MiB, avec un pilote de 512 MiB. Les sorties portent
le périmètre `full_title_tables_subset` et `PROVISIONAL_SUCCESS.json` : elles ne
valident pas les autres tables ou les cinq autres questions. Les notes nulles
sont exclues du classement. L'essai CLI contrôlé réconcilie les lignes des notes ;
les 33 tests passent après extraction des calculs communs (61,62 s).


Lors du lancement complet sur Windows, Q1, Q2 et Q3 ont abouti avant qu’une
lecture du classement échoue avec `Cannot allocate memory` pendant l’exécution
parallèle au nettoyage. Les sorties déjà achevées sont conservées. Le profil
complet des notes a réconcilié 1 731 223 lignes : aucune mise en quarantaine,
aucun doublon, aucune référence absente. La reprise ciblée `--resume-from` vérifie
les comptes des trois Parquet, les notes persistées, le manifeste, les paramètres
et l’empreinte des requêtes, puis calcule uniquement le classement dans un nouveau
dossier. Le dossier échoué reste intact. La reprise utilise `--master 'local[1]'`
et le pilote réduit ; elle fournit également les CSV complets de Q2 et Q3.

La reprise a aussi été exécutée sur le jeu contrôlé avec un pilote de 512 MiB,
un seul thread et les trois sorties déjà matérialisées ; elle a publié les
quatre résultats et les deux CSV de comparaison, sans modifier le dossier source.

### Premières analyses complètes des titres obtenues

Les sorties réelles de Q1 à Q3 et du classement sont publiées dans `validation/` :
13 640 couples film–genre pour Q1, 138 lignes pour les trois premiers genres par
année, 92 lignes de comparaison et 9 089 places au classement dans 979 groupes
année–genre. La liste dédupliquée contient 6 257 films. Les CSV conservent leurs
empreintes source. Le contrôle `title-query-verification-2026-11-11.json` vérifie
les 46 années, les bornes et rangs, les égalités et la réconciliation des totaux
des trois premiers genres. Les critères restent provisoires, et les cinq autres
requêtes attendent encore le nettoyage de leurs tables.

Pour préparer l'interface de Benjamin avant la fin des crédits non sélectionnés,
`imdb_preparation.export_selected_credits` filtre le fichier complet sur les
identifiants de films sélectionnés avant les transformations coûteuses. Il
normalise d'abord la clé de titre en Spark, puis conserve les mêmes contrôles de
clés, valeurs et personnes. Les parents viennent des tables titres/personnes
terminées. Les couples `tconst,nconst` restent distincts et non nuls ; un CSV
séparé conserve catégories et ordre pour le choix des trois personnes.

```powershell
docker compose -f compose.analysis.yaml run --rm title-analysis python -m imdb_preparation.export_selected_credits --prepared results/imdb-full-2026-11-11 --snapshot data/raw/imdb-full-2026-11-11 --raw-profile results/imdb-full-raw-profile-2026-11-11 --selected-titles results/title-queries-full-provisional-2026-11-11/selected_titles.csv --output results/selected-credits-full-provisional-2026-11-11
```

Pendant le nettoyage, adapter le chemin de préparation au dossier `.incomplete-*`
correspondant. Le résultat porte le périmètre `full_selected_credits_subset`,
jamais une certification de nettoyage global. `candidates.csv` respecte
l'interface à deux colonnes ; `credit_details.csv` et `coverage.csv` exposent les
rôles, l'ordre et les films ayant moins de trois personnes disponibles. Aucune
collecte de biographies ou de synopsis n'est effectuée. Le filtrage anticipé est
comparé au nettoyage non filtré sur un cas contrôlé avec doublons Unicode,
numéros d'ordre équivalents et clés conflictuelles : 34 tests passent (66,02 s).

Le pool réel des 6 257 films est obtenu : 124 499 crédits sélectionnés dans les
102 683 165 lignes source, 113 273 couples distincts valides, 11 225 répétitions
de couples liées aux crédits et une ligne de crédit rejetée. Aucun doublon de
ligne normalisée ni clé conflictuelle n'a été trouvé dans ce sous-ensemble.
Cinq films ont moins de trois personnes disponibles ; cette information reste
explicite dans la couverture et ne doit pas être transformée en biographies.

Le module `imdb_preparation.choose_principals` produit une entrée de travail
bornée pour le collecteur de Benjamin : un réalisateur puis deux autres
interprètes, en remplaçant les rôles manquants par d'autres crédits disponibles,
et jusqu'à deux personnes de réserve distinctes. Ce choix est provisoire et
reste `student_validated: false`. Un réalisateur également interprète n'est
compté qu'une fois par film. Le pool complet reste conservé.

```powershell
docker compose -f compose.analysis.yaml run --rm title-analysis python -m imdb_preparation.choose_principals --source results/selected-credits-full-provisional-2026-11-11 --output results/enrichment-input-full-provisional-2026-11-11
```

`primary.csv`, `reserves.csv` et leur union `enrichment-input.csv` ont exactement
les colonnes `tconst,nconst`. La couverture conserve les films ayant moins de
trois personnes. Le CSV d'union peut alimenter sans changement le collecteur de
la PR #3 ; aucun texte Wikipédia n'est recueilli par cette commande. Les 35 tests
passent (66,48 s), et la commande a également été exécutée sur un cas contrôlé
avec une seule personne disponible.

### Entrées d'enrichissement publiées pour Benjamin

La sélection de personnes réelle fournit 18 764 couples prioritaires et 12 466
couples de réserve. Leur union contient 31 230 couples distincts, correspondant
à 19 375 identifiants de personnes, pour les 6 257 films. Les cinq films ayant
moins de trois personnes dans le pool restent dans la couverture.

Les fichiers versionnés suivants peuvent être récupérés depuis la PR #5 :

- `validation/enrichment-input-full-provisional-2026-11-11.csv` : union des
  priorités et réserves, entrée directe `tconst,nconst` du collecteur de la PR #3 ;
- `validation/enrichment-primary-full-provisional-2026-11-11.csv` et
  `validation/enrichment-reserves-full-provisional-2026-11-11.csv` : lots séparés ;
- `validation/enrichment-coverage-full-provisional-2026-11-11.csv` : effectifs et
  rôles disponibles par film ;
- `validation/selected-credits-full-provisional-2026-11-11.json` et
  `validation/principal-choice-full-provisional-2026-11-11.json` : provenance et
  résultats des deux phases ;
- `validation/principal-choice-verification-2026-11-11.json` : vérification des
  identifiants, colonnes, unicité, union disjointe, effectifs par film, empreintes
  et réconciliation des crédits.

Ces entrées ne contiennent aucun synopsis ni biographie. Leur politique de
sélection reste provisoire ; le nettoyage de tout le corpus et les cinq autres
questions d'exploration restent suivis par le lanceur complet. Le brouillon
`validation/message-ben-2026-11-11.md` résume ce point de coordination.

### Retour de relecture de Benjamin — titres Wikipédia

Le cas `___` signalé sur la PR #4 est corrigé : la validation et la
déduplication appliquent la même clé de titre que `title_key` du lecteur
streaming (suites de `_` et d'espaces remplacées par un espace, puis trim).
Les titres devenus vides sont rejetés avec `invalid_reference`.
`validation/reference-title-review-2026-11-11.json` conserve le contrôle avec
les fonctions exactes du lecteur de Benjamin. Les 28 tests de la branche
intégration passent ; après propagation sur la branche d'analyse, les 36 tests
passent en 69,75 s. Cette correction ne modifie aucun pipeline de Benjamin.

### Reprise après interruption de Docker — 11 novembre

Après la perte du conteneur de calcul, les cinq premières tables terminées sont
restées dans `results/imdb-full-2026-11-11.incomplete-31348599`. Les caches Spark
intermédiaires des crédits ne constituent pas un point de reprise. La nouvelle
option reprend uniquement les tables disposant de leurs marqueurs Parquet et
contrôle schémas, comptes propres/quarantaines et agrégats d'audit avant de les
réutiliser dans un nouveau dossier temporaire. L'ancien dossier reste intact.

Les fichiers sont réutilisés par liens physiques NTFS, vérifiés dans le conteneur
sur le poste Windows : pas de deuxième copie volumineuse des tables terminées.
Le profil brut complet déjà calculé est vérifié contre le manifeste et réutilisé ;
les 39 taux ne sont pas recalculés par la reprise. Leur comparaison ultérieure
constitue alors un contrôle de cohérence avec cette référence, pas un second
calcul indépendant.

```powershell
.\run_full_imdb.ps1 -ResumeFrom results/imdb-full-2026-11-11.incomplete-31348599 -RawProfile results/imdb-full-raw-profile-2026-11-11 -LegacyAsOfYear 2026
```

`-LegacyAsOfYear` est l'assertion de l'opérateur pour ce checkpoint ancien de
cette tâche, dont le lanceur utilisait explicitement 2026. Les seuls nombres de
lignes ne prouvent pas cryptographiquement sa provenance initiale. L'adoption est
tracée comme `legacy_context_asserted` dans le résumé. Les nouvelles exécutions
écrivent avant traitement `checkpoint_context.json`, contenant année, empreinte
du manifeste et empreintes canoniques des règles de qualité et du schéma, ainsi
que le manifeste d'entrée. Une reprise moderne refuse une divergence de ces
paramètres ou des règles ; un checkpoint ancien exige son année explicite.

Le lanceur archive le journal interrompu, puis reprend préparation, exploration
et exports avec la surveillance du disque existante. Il repartitionne aussi les
champs bruts sur 64 partitions avant la normalisation Unicode : la lecture gzip
reste séquentielle, tandis que les transformations suivantes peuvent utiliser les
quatre threads. Les règles de nettoyage et les seuils ne changent pas.

### Exécution indépendante du PC avec GitHub Actions

Le workflow `.github/workflows/imdb-cloud.yml` propose une exécution sur un
runner GitHub `ubuntu-24.04`. Il ne dépend pas de Desktop Commander ni du PC
Windows. Il exige au moins quatre CPU, 12 Gio de mémoire et 25 Gio de disque
libre après retrait des SDK inutilisés du runner jetable. Il installe Python
3.13, Java 21 et Spark 4.2.0 et lance les tests contrôlés avant le lot complet.

Sur la branche de Nicolas uniquement, une modification du workflow ou de
`.github/imdb-cloud-request.json` déclenche le traitement. Une modification du
code seule ne lance pas de lot lourd. `workflow_dispatch` est aussi déclaré,
mais ce déclenchement manuel exige que le workflow soit sur la branche par
défaut ; aucune fusion de main n'est nécessaire pour le déclenchement par push.

Les sept téléchargements sont vérifiés. Si leurs empreintes sont identiques au
lot mesuré le 11 novembre, les 39 taux bruts sont réutilisés avec cette preuve
de concordance. Si IMDb a publié une mise à jour, les 39 colonnes du nouveau lot
sont profilées de nouveau et les anciens compteurs ne sont pas repris. Cette
provenance figure dans les résultats. Le nettoyage repart du début sur le runner,
sans prétendre réutiliser les cinq tables conservées sur Windows.
Les langues originales font l'objet d'une nouvelle collecte P364 avec manifeste.

La limite de 350 minutes (5 h 50) s’applique à chaque job de calcul, pas à
l’ensemble de la chaîne. Le pipeline est découpé comme décrit ci-dessous.
Une exécution échouée conserve les diagnostics disponibles, sans annoncer
le succès du traitement. Un succès publie des artefacts de résultats dérivés,
avec marqueur `VERIFIED.json`, pour sept jours : qualité, analyses, classement,
crédits sélectionnés et couverture. Les sept tables sources et les tables
nettoyées complètes ne sont pas publiées. Les checkpoints de préparation passent entre jobs via des caches GitHub
propres à cette exécution ; une reprise peut conserver les phases amont.

L'ajout du fichier de workflow peut exiger la portée OAuth GitHub `workflow` en
plus de `repo`. Une exécution n'est considérée indépendante du PC qu'après
vérification de son démarrage effectif sur GitHub. La fermeture de ce chat ne
constitue pas une garantie de conservation de son environnement de calcul.

Le paquet cloud conserve aussi `integration/imdb_ids`,
`integration/movie_catalog` et `integration/clean/title.ratings` pour les jointures
du binôme et l'étude des seuils. Les tables IMDb nettoyées complètes autres que
les notes ne sont pas publiées. L'empreinte du profil est comparée à l'empreinte
originale du manifeste tracée dans `checkpoint_context.json` : une réécriture
JSON du manifeste dans la préparation ne change pas la provenance des données.
Un contrôle d'intégration synthétique traverse profil, préparation, neuf sorties
d'analyse, exports et paquet final ; il vérifie également les empreintes des
fichiers publiés et le refus d'un profil provenant d'un autre manifeste.

### Traitement nocturne réparti sur plusieurs jobs GitHub

Un runner GitHub hébergé standard limite un job à six heures. L'ancien job
monolithique a été remplacé par cette chaîne, qui peut dépasser six heures au
total sans dépendre du PC :

1. `inputs` : tests, téléchargement d'un seul lot, profil des 39 colonnes et
   sauvegarde du lot dans un cache identifié par `github.run_id`.
2. `parents` : nettoyage des titres et des personnes, puis checkpoint validé.
3. `relations` : restauration du même lot et du checkpoint ; ajout des titres
   alternatifs, équipes et épisodes, puis checkpoint des cinq tables.
4. `principals` : restauration du même lot et des cinq tables ; crédits et notes,
   puis publication du marqueur global `SUCCESS.json` et du checkpoint complet.
5. `analysis` : restauration des sept tables et des petites références ; huit
   questions, classement, exports et publication des résultats vérifiés.

Un sixième job `languages` collecte la référence P364 en parallèle des entrées.
Il ne bloque pas les phases de nettoyage si son service est indisponible.
L'analyse attend sa référence avec manifeste. Les profils et références, petits,
sont conservés en artefacts séparés pour ne pas dépendre de l'éviction des anciens
caches lourds. Les limites des jobs d'entrée/référence sont 350 et 20 minutes ;
chaque job de calcul a sa propre limite de 350 minutes. Ces limites ne prédisent
pas la durée totale et ne garantissent pas une fin avant le matin.

`prepare --stop-after name.basics` et `--stop-after title.episode` publient un
préfixe terminé, avec `CHECKPOINT.json`, contexte, compteurs et schémas, sans
`SUCCESS.json`. Les consommateurs refusent ce préfixe incomplet. `--resume-from`
contrôle l'année, l'empreinte originale du manifeste, les règles, marqueurs et
audits, puis réutilise les tables immuables. Les sorties des trois phases sont
comparées au calcul continu sur la fixture : mêmes tables, compteurs et profils.

Les clés de cache ne comportent aucune substitution vers un autre lot. Une perte
de cache nécessaire arrête explicitement la phase ; elle ne provoque pas un
téléchargement silencieux d'un lot du lendemain. Les caches restent soumis au
quota et à l'éviction GitHub. Le lot brut n'est téléchargé qu'au job d'entrée ;
les phases suivantes ne dépendent donc pas d'une mise à jour quotidienne IMDb.
Les caches intermédiaires sont destinés à la reprise et ne remplacent pas les
artefacts de résultats dérivés conservés sept jours. Une relance des seuls jobs
en échec peut réutiliser les étapes amont tant que leurs caches sont disponibles.

### Finition automatique après le calcul, sans agent de chat actif

Le workflow `imdb-finalize.yml` est déclenché explicitement sur la branche de
Nicolas et partage le groupe de concurrence du workflow de calcul. Il reste
en attente sans runner pendant ce calcul, puis démarre sa propre machine.
La requête `.github/imdb-finalize-request.json` fixe exactement le run amont
et son commit. Le calcul déjà lancé n'est ni modifié ni redémarré.

La finition exige que les sept tables soient prêtes. Elle restaure leur cache,
télécharge les références et résultats du run désigné et vérifie les empreintes.
Si la préparation a réussi mais que l'analyse ou l'export a échoué, une seule
reprise bornée de ces étapes est prévue ; une préparation incomplète est refusée.
Elle vérifie le classement par un calcul indépendant sur le catalogue et compare
les combinaisons durée 40/60/90 minutes et votes 100/1 000/10 000, plus les
paramètres actuels s'ils sont différents. Les effectifs et la disponibilité de
trois candidats sont mesurés par année–genre et par film distinct. Ce contrôle
porte sur des candidats, pas sur la disponibilité réelle de biographies.

`finalize.py` réutilise la politique de trois personnes prioritaires et deux
réserves, produit les CSV bornés, la couverture, les changements de films/couples
et les nouvelles personnes par rapport aux entrées déjà livrées à Benjamin.
Le cache Wikimedia de Benjamin reste à conserver. Les anciennes entrées ne
sont pas écrasées. La provenance, les empreintes et `student_validated: false`
restent explicites dans `READY_FOR_REVIEW.json`.

Les anciens paquets peuvent déclarer les empreintes de sidecars Hadoop `.crc`
que l'envoi d'artefacts masque par défaut. Leur absence est documentée et tolérée
uniquement pour ces fichiers auxiliaires cachés ; chaque fichier de données
déclaré doit être présent et correspondre à son SHA256. Un contenu modifié ou
un chemin sortant de l'artefact est refusé.

La finition sauvegarde un artefact pour sept jours et peut committer les petits
CSV/JSON dans `validation/overnight-<run-amont>/` sur la seule branche
`feat/imdb-full-analysis`, avec l'identité de Nicolas déjà convenue. Si cette
branche a changé depuis le lancement de la finition, elle conserve les fichiers
en artefact au lieu de forcer un push. Aucun merge, aucune approbation, aucun
changement aux branches de Benjamin et aucun texte de REPORT.md n'est effectué.

Ce sont des actions programmées, pas un assistant qui reprend spontanément
la conversation : une panne imprévue reste à diagnostiquer. La validation humaine
des critères et des interprétations, les relectures croisées et l'essai intégré
restent nécessaires avant de déclarer le projet terminé.

### Reprise automatique après une panne de diagnostics GitHub

Le 11 novembre, le calcul titres/personnes et son cache ont réussi, mais le
service d'artefacts a renvoyé du HTML au lieu de JSON pendant l'envoi des logs.
Cette erreur de diagnostics a bloqué les jobs dépendants. La tentative 2 du
batch a été lancée ; elle conserve le même lot et le même commit source.

La finition attend désormais le batch puis cherche ses checkpoints exacts.
Si les sept tables sont présentes, elle passe directement à la vérification.
Sinon, deux jobs séparés reprennent les relations depuis le checkpoint des
parents, puis les crédits et notes depuis celui des cinq tables. Chaque calcul
utilise le commit source original, le même manifeste et les contrôles de reprise
existants. Aucun téléchargement d'un nouveau lot n'est effectué en récupération.
Un cache requis absent bloque explicitement la reprise.

Les diagnostics sont tentés avec une limite de trois minutes et ne bloquent plus
les étapes utiles. Les résultats vérifiés de finition sont sauvegardés dans un
cache propre au run, puis en artefact ; les petits CSV/JSON sont aussi publiés
sur la branche prévue lorsque son commit n'a pas bougé. Une panne d'artefacts
reste signalée après cette tentative de sauvegarde. Ces mécanismes ne garantissent
pas l'absence de panne de calcul ou d'éviction de cache. La validation de syntaxe
et du graphe figure dans validation/cloud-recovery-validation-2026-11-11.json ;
le succès sur le lot réel reste à contrôler dans les runs GitHub.

### Vérification des sorties d'exploration et tableaux de relecture

La finition appelle maintenant `imdb_preparation.review_outputs` après le handoff.
Ce contrôle relit les Parquet matérialisés, leur compteur déclaré et les marqueurs
SUCCESS. Il lie les analyses au résumé exact de préparation, contrôle les clés et
les domaines de valeurs des huit questions, les rangs et leurs départages, puis
réconcilie Q2/Q3 et la sélection du classement. Il vérifie les invariants des
sorties sans prétendre recalculer indépendamment toutes les requêtes sources.

```bash
python -m imdb_preparation.review_outputs --prepared results/cloud-prepared --analysis artifacts/verified/analysis --output artifacts/query-review --preview-rows 100
```

`QUERY_REVIEW.json` contient les comptes complets et les contrôles. Chaque table
a aussi un aperçu CSV et JSON borné à 100 lignes, avec les tableaux imbriqués
sérialisés en JSON pour le CSV. Ce sont des aperçus techniques, les résultats
intégraux restent dans les Parquet de l'artefact. Le contrôle ne valide pas les
choix méthodologiques à la place des étudiants et ne rédige pas REPORT.md.
Les petits tableaux sont ajoutés à la branche de sortie si celle-ci est inchangée.
Même si une validation échoue, les sorties déjà disponibles sont sauvegardées en
cache/artefact pour le diagnostic ; la publication sur la branche reste bloquée.

La commande `python validation/check_query_review.py` a été exécutée dans le
worktree d'intégration Docker avec les sorties synthétiques existantes sous
`results/cross-pipeline-v1`. Elle accepte les neuf tables et refuse des rangs de
popularité inversés, un réalisateur ayant deux films très bien notés et des
compteurs modifiés. La preuve est dans
`validation/query-review-validation-2026-11-11.json`. Ce test ne constitue pas
une validation du lot réel en cours sur GitHub.

### Référence Wikidata tronquée — reprise du 12 novembre

Le nettoyage des sept tables du run 43093003953 a réussi. L'analyse a ensuite
refusé le CSV Wikidata, dont le dernier enregistrement était un simple `t`.
Le collecteur historique contrôlait l'en-tête mais pas chaque largeur de ligne.
La lecture Spark stricte est conservée : aucune ligne tronquée n'est ignorée
silencieusement pour faire passer le calcul.

La collecte P364 utilise désormais SELECT DISTINCT et des pages ordonnées de
20 000 lignes, avec une pause entre les pages. Chaque réponse est vérifiée avant
publication (CSV strict, largeur des lignes, longueur HTTP lorsqu'elle est fournie),
avec reprises bornées des erreurs temporaires et respect de Retry-After. Les
pages identiques répétées sont refusées ; les doublons éventuels des déclarations
restent comptés et traités par Spark. La provenance conserve paramètres,
nombres de lignes et empreintes des réponses ; les pages ne constituent pas une
transaction atomique de Wikidata et la couverture IMDb reste explicitement mesurée.

La finition valide la référence restaurée. Si elle est absente, tronquée ou issue
de l'ancien collecteur non paginé, elle collecte un nouveau fichier
`recovered-original-languages.csv` et son propre manifeste. Le fichier original
reste intact. Les sept tables préparées sont restaurées depuis leur cache exact ;
seules l'analyse et les sorties dérivées reprennent. Le nouveau CSV, son manifeste
et son vocabulaire sont conservés sous `artifacts/reference` dans la livraison.
Les artefacts finaux sont conservés 90 jours.

Les neuf tests de langues ont réussi dans Docker sous Spark 4.2.0. Les huit tests
sans Spark ont aussi réussi en natif après suppression de la dépendance Spark
inutile du collecteur de données. La preuve et les périmètres figurent dans
`validation/language-pagination-recovery-2026-11-12.json`. Le succès de la nouvelle
collecte et des analyses réelles reste à vérifier dans le workflow de reprise.


### Snapshot de langues vérifié, restauration sans réseau

Le snapshot original du 11 novembre sur le PC comporte 369 502 enregistrements,
aucune ligne CSV mal formée et une empreinte conforme au manifeste d'origine.
Son vocabulaire et son manifeste sont archivés avec lui dans
`references/wikidata-p364-2026-11-11.tar.gz` (environ 3,2 Mo, faits Wikidata CC0).
La reprise utilise cette archive vérifiée lorsque la référence du run est
invalide. Les octets, la date et le manifeste d'origine sont conservés : elle
n'est pas présentée comme une nouvelle collecte paginée. Un reçu de réutilisation
est ajouté à `artifacts/reference/REFERENCE_REUSE.json`.

La collecte paginée reste disponible pour créer un nouveau snapshot. Elle ne
compare plus l'ordre lexical des chaînes CSV à l'ordre des termes RDF de SPARQL
(par exemple, nœuds anonymes et URI) : ce sont deux ordres distincts. Elle détecte
les pages identiques répétées par empreinte, tandis que les doublons de données
et les langues non identifiées sont réconciliés dans Spark.

### Calcul de la question libre sur les tables complètes déjà préparées

Le workflow `.github/workflows/imdb-collaborations-full.yml` exécute le code de
Benjamin au commit immuable `8a0d75bb26d6437ead2c927d69da7b8bcff6592d`, sans
modifier ses fichiers ni ses branches. Il réutilise le cache exact des sept
tables du run `43093003953`. Le manifeste source et le résumé préparé sont
vérifiés par leurs SHA-256 avant exécution. Aucun nouveau nettoyage n'est lancé.

L'analyse conserve l'historique complet jusqu'à 2025 pour établir la première
année observée et compare les notes dans la période 1980–2025. Elle produit dix
comparaisons : votes 0/100/1000/5000/10000 et historique de 2/3 années. Les films
sans note et ceux d'avant 1980 restent utiles à la chronologie. Ce calcul ne
limite pas l'historique au top 10 d'enrichissement.

L'audit DuckDB 1.4.3 de Benjamin recalcule indépendamment les diagnostics,
effectifs et moyennes depuis les quatre mêmes Parquet. Le marqueur
`VERIFIED_FOR_PEER_REVIEW.json` est écrit uniquement si ses dix comparaisons
passent. Les artefacts sont conservés 90 jours, même si un contrôle échoue ;
l'existence d'un artefact ne signifie donc pas à elle seule une réussite.
Les CSV/JSON vérifiés peuvent être publiés sous
`validation/collaborations-full-43093003953/` sur la branche de Nicolas si elle
n'a pas changé. Le code de Benjamin et ses commits restent attribués à leur
auteur ; sa PR, sa relecture et l'interprétation étudiante restent distinctes.

### CSV complets pour la relecture

Après extraction de l'artefact vérifié, installer `duckdb==1.4.3` dans un
environnement Python séparé puis lancer, depuis la racine :

```bash
python scripts/export_verified_query_csv.py --verified-root results/final/verified --output results/full-query-csv
```

Le dossier de sortie doit être neuf. Le script vérifie les empreintes du paquet
(les sidecars Hadoop CRC facultatifs sont exclus des artefacts GitHub), exporte
les huit requêtes et le classement intégral, puis relit chaque CSV et compare
son nombre de lignes au résultat déclaré. Les tableaux sont des cellules JSON.
Ces CSV sont complets ; les aperçus de `query-review/` restent bornés à 100 lignes.
La conversion a passé sur les sorties réelles, dans le cloud et sur Windows.
La provenance et les comptes sont conservés dans `COMPLETE_CSV_EXPORT.json`.
Le prompt de transmission actualisé est
`validation/message-ben-final-2026-11-12.md` ; aucun message n'est envoyé.
