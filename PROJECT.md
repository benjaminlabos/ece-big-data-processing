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
