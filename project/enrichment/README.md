# Enrichissement IMDb

Le pilote enrichit deux films et sept crédits à partir de Wikipédia en anglais. Il rapproche chaque identifiant IMDb de la propriété [P345 de Wikidata](https://www.wikidata.org/wiki/Property:P345), puis récupère la section `Plot` ou `Synopsis` du film et le début de l’article biographique. Les jointures et le nettoyage du HTML s’exécutent dans Spark.

## Entrée à fournir

Un CSV UTF-8 avec exactement les colonnes `tconst,nconst`. Chaque ligne désigne un film sélectionné et une personne créditée. Les identifiants proviennent de `title.basics` et de `title.principals` ou `name.basics`. Un film peut apparaître sur plusieurs lignes. Spark écarte les couples identiques et signale les identifiants mal formés.

Le fichier [candidates.csv](candidates.csv) utilise trois membres de la distribution de chacun des deux films et un crédit supplémentaire de coproduction pour vérifier le cas d’une biographie indisponible. Les crédits sont consultables sur [IMDb pour Inception](https://www.imdb.com/title/tt1375666/fullcredits/) et [IMDb pour The Dark Knight](https://www.imdb.com/title/tt0468569/fullcredits/).

L’export de la sélection fournit un CSV `tconst` avec tous les films, y compris ceux qui n’ont aucun crédit candidat. L’entrée `tconst,nconst` contient les crédits prioritaires et les réserves. Le fichier des prioritaires, au même format, sert à mesurer l’apport des réserves. Le classement par année et genre fournit les colonnes `startYear,genre,genre_year_rank,tconst,primaryTitle,runtimeMinutes,averageRating,numVotes`. Ses critères restent provisoires jusqu’à leur validation par le binôme. [Exports IMDb de la PR #5](https://github.com/benjaminlabos/ece-big-data-processing/tree/0f82d8349a9dae7c9c43fea53fc559d4bd8e9b23/validation)

## Exécution

Depuis ce dossier, avec un environnement PySpark 4.x disponible. Spark 4.2.0 requiert Python 3.10 ou ultérieur. Installer `beautifulsoup4` aussi dans l’environnement Python des workers Spark. [Prérequis Spark 4.2.0](https://spark.apache.org/docs/4.2.0/)

```sh
python -m pip install -r requirements.txt
python collect.py --candidates candidates.csv --cache work/raw
spark-submit --master 'local[2]' enrich.py --candidates candidates.csv --raw 'work/raw/*.json' --output work/results
```

La collecte effectue des requêtes en série, attend au moins une seconde avant chaque requête et demande au plus 25 identifiants IMDb par requête Wikidata. Elle respecte les délais `Retry-After` et `maxlag`. Elle recherche chaque personne une fois par cache, quel que soit le nombre de films associés. Le paramètre `--user-agent` accepte un nom d’application et une URL ou une adresse de contact. En cas d’indisponibilité persistante, la commande s’arrête et indique le délai de relance quand la source en fournit un. Relancer la même commande reprend les identifiants manquants ou dont le JSON est endommagé. Les absences confirmées par la source sont conservées en cache. Exécuter un seul collecteur par dossier. `--max-new N` permet un premier passage borné. [Règles de l’API MediaWiki](https://www.mediawiki.org/wiki/API:Etiquette)

Avec l’image PySpark du TP Wikimedia déjà construite, depuis la racine du dépôt dans PowerShell après la collecte locale.

```powershell
docker compose -f 05.structured-streaming/lab_docker_wikistreams/compose.yaml run --rm --no-deps `
  -v "${PWD}/project/enrichment:/home/jovyan/enrichment" `
  -w /home/jovyan/enrichment pyspark-notebook `
  bash -c 'python -m pip install --no-cache-dir -r requirements.txt && spark-submit --master local[2] enrich.py --candidates candidates.csv --raw work/raw --output work/results'
```

## Exécution sur la sélection provisoire

Depuis la racine du dépôt, extraire les exports dans un worktree séparé.

```powershell
git fetch origin feat/imdb-full-analysis
git worktree add --detach ../imdb-inputs-nicolas 0f82d8349a9dae7c9c43fea53fc559d4bd8e9b23
$inputs = (Resolve-Path ../imdb-inputs-nicolas/validation).Path
Set-Location project/enrichment
python -m pip install -r requirements.txt
$suffix = 'full-provisional-2026-11-11.csv'
python collect.py --titles "$inputs/selected-titles-$suffix" `
  --candidates "$inputs/enrichment-input-$suffix" --cache work/provisional/raw
```

### Reprendre le cache Windows

Pour reprendre sur le même PC, relancer la commande ci-dessus avec les mêmes CSV et le même dossier `work/provisional/raw`, après l’arrêt du collecteur précédent. Le processus Python Windows a besoin d’un PC allumé et connecté à Internet. Docker sert à transformer le cache dans Spark. Une mise en veille suspend les requêtes et une extinction ou fermeture de session met fin à ce lancement.

La sauvegarde ZIP contient un JSON finalisé par identifiant. Vérifier son SHA-256 puis la décompresser dans un dossier neuf avant de l’utiliser. Le cache de travail existant reste disponible pendant la restauration.

```powershell
Get-FileHash work/provisional/backups/cache-2026-11-11-4247.zip -Algorithm SHA256
Expand-Archive work/provisional/backups/cache-2026-11-11-4247.zip work/provisional/restored-4247
python collect.py --titles "$inputs/selected-titles-$suffix" `
  --candidates "$inputs/enrichment-input-$suffix" --cache work/provisional/restored-4247
```

Conserver le ZIP et son reçu hors Git. Lors d’un changement de sélection, vérifier les ensembles d’identifiants et les empreintes des nouveaux CSV. Relancer sur un cache existant conserve les réponses valides pour les identifiants communs.

Le calcul Spark peut être exécuté pendant la collecte. Il utilise les fichiers JSON finalisés qui étaient visibles au début de la lecture et crée un nouveau dossier à chaque exécution. Choisir un nom différent pour un deuxième instantané.

Pour lire un seul fichier dans Spark, figer les réponses finalisées avec la commande ci-dessous. Elle affiche le nombre d’enregistrements et l’empreinte SHA-256. Passer ensuite `--raw work/provisional/cache-snapshot.json` à la commande Spark.

```powershell
python freeze_cache.py --cache work/provisional/raw --output work/provisional/cache-snapshot.json
```

```powershell
spark-submit --master 'local[2]' enrich.py `
  --titles "$inputs/selected-titles-$suffix" --candidates "$inputs/enrichment-input-$suffix" `
  --primary "$inputs/enrichment-primary-$suffix" --top "$inputs/top10-$suffix" `
  --raw work/provisional/raw --output work/provisional/snapshot-01
```

Sous Docker Desktop, la même commande s’exécute dans l’image du TP Wikimedia déjà construite. Depuis `project/enrichment`, conserver la variable `$inputs` définie plus haut.

```powershell
$job = (Get-Location).Path
docker run --rm --pull=never --cpus 2 --memory 2500m `
  --mount "type=bind,source=$job,target=/job" `
  --mount "type=bind,source=$inputs,target=/inputs,readonly" `
  -w /job tp05-wikistreams-notebook:local spark-submit --master 'local[2]' `
  --driver-memory 1g --conf spark.sql.shuffle.partitions=8 --conf spark.ui.enabled=false enrich.py `
  --titles "/inputs/selected-titles-$suffix" --candidates "/inputs/enrichment-input-$suffix" `
  --primary "/inputs/enrichment-primary-$suffix" --top "/inputs/top10-$suffix" `
  --raw work/provisional/raw --output work/provisional/snapshot-01
```

Les tests rapides du contrat CSV, du cache et des temporisations se lancent avec `python -m unittest -v`.

## Sélection révisée du 12 novembre

Le lot commun contient 6 256 films, 19 374 personnes et 31 225 crédits. La comparaison avec le lot du 11 novembre retrouve un film retiré, six crédits retirés et un crédit ajouté. Le nouvel identifiant est `nm19101446`. Conserver les réponses déjà collectées, y compris celles hors de cette sélection. Attendre l’arrêt du collecteur qui utilise les anciens CSV avant de lancer les commandes sur le même cache. [Exports révisés](https://github.com/benjaminlabos/ece-big-data-processing/tree/7790935f6db72fdd223b47b35bcff02f62ffdfb3/validation/overnight-43093003953)

Depuis la racine du clone sous PowerShell.

```powershell
git fetch origin feat/imdb-full-analysis
git worktree add --detach ../imdb-handoff-2026-11-12 7790935f6db72fdd223b47b35bcff02f62ffdfb3
$inputs = (Resolve-Path ../imdb-handoff-2026-11-12/validation/overnight-43093003953).Path
Set-Location project/enrichment
python collect.py --titles "$inputs/selected-titles.csv" --candidates "$inputs/enrichment-input.csv" --cache work/provisional/raw
python freeze_cache.py --cache work/provisional/raw --output work/cache-revised.json > work/cache-revised.receipt.json
$job = (Get-Location).Path
docker run --rm --pull=never --cpus 4 --memory 4000m `
  --mount "type=bind,source=$job,target=/job" `
  --mount "type=bind,source=$inputs,target=/inputs,readonly" `
  -w /job tp05-wikistreams-notebook:local spark-submit --master 'local[4]' `
  --driver-memory 2g --conf spark.sql.shuffle.partitions=8 --conf spark.ui.enabled=false enrich.py `
  --titles /inputs/selected-titles.csv --candidates /inputs/enrichment-input.csv `
  --primary /inputs/primary.csv --top /inputs/top10.csv `
  --raw work/cache-revised.json --output work/coverage-revised
```

### Relire la couverture

Depuis le même dossier, relire les quatre Parquet avec PyArrow. Le contrôle compare chaque réponse figée à son statut et à sa provenance en sortie. Il recalcule la couverture par film et par groupe à partir des crédits détaillés.

```powershell
python -m pip install pyarrow
python audit_outputs.py work/coverage-revised "$inputs/selected-titles.csv" `
  "$inputs/enrichment-input.csv" "$inputs/primary.csv" "$inputs/top10.csv" `
  --snapshot work/cache-revised.json --receipt work/cache-revised.receipt.json `
  --require-complete --biography-gaps work/coverage-revised-biography-gaps.csv > work/coverage-revised-audit.json
```

Pour un instantané en cours de collecte, omettre `--require-complete`. Choisir de nouveaux chemins à chaque exécution. Le CSV des manques donne pour chaque film le nombre de biographies à trouver et les statuts des crédits concernés. Le JSON d’audit compte aussi les films auxquels il manque un synopsis, les films auxquels il manque des biographies et les films auxquels il manque les deux. Conserver l’instantané et les textes hors Git.

L’instantané intermédiaire de 13 782 réponses a été sauvegardé avec son empreinte, puis transformé sous Spark 4.2.0. Les quatre Parquet ont été relus et les 979 groupes année–genre ont été recoupés avec les CSV de sélection. On obtient 3 081 synopsis, 7 918 biographies distinctes et 3 488 films qui atteignent trois biographies, dont 662 grâce aux réserves. Cinq films ont moins de trois candidats dans la sélection. [Instantané révisé et contrôle des Parquet](evidence/revised-snapshot-2026-11-12.json)

## Sorties

- `work/results/films` contient une ligne par film.
- `work/results/principals` contient une ligne par couple film–personne.
- `work/results/coverage` compte les personnes candidates et les biographies disponibles par film. Avec `--primary`, il compte aussi les biographies prioritaires disponibles et mesure combien de films atteignent trois biographies grâce aux réserves.
- Avec `--top`, `work/results/coverage_by_year_genre` agrège la couverture des films par année et genre. Un film peut appartenir à plusieurs groupes.
- `_MANIFEST.json` contient les empreintes des CSV lus, les nombres d’entrées et les statuts relus en Parquet. Le dossier apparaît après publication de toutes les tables. `_COMPLETE` est présent lorsque tous les identifiants demandés ont une réponse en cache, y compris les absences documentées.
- `text` contient le texte extrait. `source_url` identifie la révision de l’article, `wikidata_url` l’entité ayant fourni le lien et `retrieved_at_utc` la date de collecte.
- `missing_reason` explique les absences. Par exemple, `no_wikidata_item` indique que la requête par identifiant IMDb n’a renvoyé aucune entité et `not_collected` signale que le cache ne contient aucune réponse pour cet identifiant. Les jointures conservent ces lignes.
- Le texte provient des contributeurs de Wikipédia. L’extraction retire le balisage et les renvois de citation. Conserver les champs de provenance et de licence CC BY-SA 4.0 lors de la réutilisation. [Conditions Wikimedia](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use#7._Licensing_of_Content)

Le dossier `work/` contient le cache et les résultats générés localement.

## Contrôle de l’essai

Les appels en direct à Wikidata et à Wikipédia ont fourni huit révisions pour neuf identifiants recherchés. Le 10 novembre 2026, Spark 4.0.1 a nettoyé les fragments, effectué les deux jointures et relu les résultats Parquet sous Ubuntu WSL. Les sorties contiennent deux synopsis et six introductions biographiques, soit trois par film, avec leur URL de révision. La ligne de Jordan Goldberg, coproducteur crédité sur *Inception*, conserve le statut `no_wikidata_item`.

Le même cache a ensuite été retraité sous Spark 4.2.0 et Java 21 sous Ubuntu WSL. Les contenus, les champs de provenance et le statut d’absence sont identiques dans les deux sorties Parquet.

| IMDb | Article vérifié | Fragment |
| --- | --- | --- |
| `tt1375666` | [Inception](https://en.wikipedia.org/w/index.php?oldid=1384199679) | Plot |
| `tt0468569` | [The Dark Knight](https://en.wikipedia.org/w/index.php?oldid=1384066085) | Plot |
| `nm0000138` | [Leonardo DiCaprio](https://en.wikipedia.org/w/index.php?oldid=1384722829) | Lead |
| `nm0330687` | [Joseph Gordon-Levitt](https://en.wikipedia.org/w/index.php?oldid=1383090196) | Lead |
| `nm0680983` | [Elliot Page](https://en.wikipedia.org/w/index.php?oldid=1382483559) | Lead |
| `nm0000288` | [Christian Bale](https://en.wikipedia.org/w/index.php?oldid=1383094254) | Lead |
| `nm0005132` | [Heath Ledger](https://en.wikipedia.org/w/index.php?oldid=1384065258) | Lead |
| `nm0001173` | [Aaron Eckhart](https://en.wikipedia.org/w/index.php?oldid=1384567512) | Lead |
| `nm2044373` | [Jordan Goldberg sur IMDb](https://www.imdb.com/name/nm2044373/) | `no_wikidata_item` |

## Contrôle sur les exports provisoires

Le 11 novembre 2026, un essai couvre 13 films issus des exports de la PR #5, dont huit films répartis de 1980 à 2025 et les cinq films signalés avec moins de trois candidats. Les 61 réponses en cache se répartissent entre 13 films et 48 personnes. Les 21 fichiers présents après une interruption volontaire ont conservé leur empreinte SHA-256 et leur date de modification après la reprise.

Spark 4.2.0 sous Docker Desktop a produit six synopsis et 33 biographies. Sept films ont au moins trois biographies, dont deux grâce aux réserves. Les quatre tables Parquet ont été relues et la couverture a été recalculée indépendamment à partir des crédits, des statuts et des appartenances année–genre. Les cas d’absence et les références de révision figurent dans la [preuve du pilote](evidence/provisional-pilot-2026-11-11.json).

Pour reproduire le pilote, filtrer les quatre CSV d’entrée sur les `film_ids` de la preuve et exécuter les commandes de la sélection provisoire avec ces fichiers et un cache dédié. Le premier passage utilisait `--max-new 15`. Une relance sur le même cache après interruption a complété les identifiants restants.

L’[instantané du lot provisoire](evidence/provisional-snapshot-2026-11-11.json) couvre les 6 257 films et les 31 230 crédits candidats. Il indique les réponses en cache au moment de la lecture, les informations disponibles et les demandes en attente. Le [contrôle Arrow ciblé](evidence/arrow-provisional-2026-11-11.json) a aussi passé trois cas dans Docker avec la fonction d’extraction de cette version.

L’[instantané suivant et son reçu de sauvegarde](evidence/provisional-continuation-2026-11-11.json) portent sur 4 247 réponses figées. Les quatre tables Parquet ont été relues et la couverture a été recalculée à partir des lignes détaillées. Les sept exemples vérifient les identifiants IMDb dans Wikidata et les numéros de révision de Wikipédia en anglais.

OpenAI Codex a aidé à préparer les échantillons, écrire la collecte et les transformations Spark, vérifier la reprise, contrôler les URL et les statuts d’absence, calculer la couverture et documenter l’exécution. À inclure dans la déclaration du `PROJECT.md` du groupe.
