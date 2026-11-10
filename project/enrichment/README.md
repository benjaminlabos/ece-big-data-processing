# Essai d’enrichissement IMDb

Le pilote enrichit deux films et sept crédits à partir de Wikipédia en anglais. Il rapproche chaque identifiant IMDb de la propriété [P345 de Wikidata](https://www.wikidata.org/wiki/Property:P345), puis récupère la section `Plot` ou `Synopsis` du film et le début de l’article biographique. Les jointures et le nettoyage du HTML s’exécutent dans Spark.

## Entrée à fournir

Un CSV UTF-8 avec exactement les colonnes `tconst,nconst`. Chaque ligne désigne un film sélectionné et une personne créditée. Les identifiants proviennent de `title.basics` et de `title.principals` ou `name.basics`. Un film peut apparaître sur plusieurs lignes. Spark écarte les couples identiques et signale les identifiants mal formés.

Le fichier [candidates.csv](candidates.csv) utilise trois membres de la distribution de chacun des deux films et un crédit supplémentaire de coproduction pour vérifier le cas d’une biographie indisponible. Les crédits sont consultables sur [IMDb pour Inception](https://www.imdb.com/title/tt1375666/fullcredits/) et [IMDb pour The Dark Knight](https://www.imdb.com/title/tt0468569/fullcredits/).

Pour l’étape complète, le classement Spark fournit les films sélectionnés parmi les longs métrages de 1980 à 2025, dans chaque groupe année–genre. L’export doit contenir les couples de crédits candidats pour ces films. Le classement et la sélection finale des crédits restent à brancher sur les tables nettoyées du projet. Pour chercher au moins trois biographies par film, l’export peut proposer davantage de candidats. Le CSV est indépendant des titres et des noms des articles.

## Exécution

Depuis ce dossier, avec Python 3.9+ et un environnement PySpark 4.x disponible. Installer `beautifulsoup4` aussi dans l’environnement Python des workers Spark.

```sh
python -m pip install -r requirements.txt
python collect.py --candidates candidates.csv --cache work/raw
spark-submit --master 'local[2]' enrich.py --candidates candidates.csv --raw 'work/raw/*.json' --output work/results
```

La collecte effectue des requêtes en série, attend au moins une seconde avant chaque requête et demande au plus 25 identifiants IMDb par requête Wikidata. Elle respecte les délais `Retry-After` et `maxlag`, et réutilise les réponses enregistrées. Le paramètre `--user-agent` accepte un nom d’application et une URL ou une adresse de contact. En cas d’indisponibilité persistante, la commande échoue en conservant les réponses déjà enregistrées. Relancer avec un nouveau dossier de cache pour refaire les requêtes. [Règles de l’API MediaWiki](https://www.mediawiki.org/wiki/API:Etiquette)

Avec l’image PySpark du TP Wikimedia déjà construite, depuis la racine du dépôt dans PowerShell après la collecte locale.

```powershell
docker compose -f 05.structured-streaming/lab_docker_wikistreams/compose.yaml run --rm --no-deps `
  -v "${PWD}/project/enrichment:/home/jovyan/enrichment" `
  -w /home/jovyan/enrichment pyspark-notebook `
  bash -c 'python -m pip install --no-cache-dir -r requirements.txt && spark-submit --master local[2] enrich.py --candidates candidates.csv --raw work/raw --output work/results'
```

## Sorties

- `work/results/films` contient une ligne par film.
- `work/results/principals` contient une ligne par couple film–personne.
- `text` contient le texte extrait. `source_url` identifie la révision de l’article, `wikidata_url` l’entité ayant fourni le lien et `retrieved_at_utc` la date de collecte.
- `missing_reason` explique les absences. Par exemple, `no_wikidata_item` indique que la requête par identifiant IMDb n’a renvoyé aucune entité et `not_collected` signale que le cache ne contient aucune réponse pour cet identifiant. Les jointures conservent ces lignes.
- Le texte provient des contributeurs de Wikipédia. L’extraction retire le balisage et les renvois de citation. Conserver les champs de provenance et de licence CC BY-SA 4.0 lors de la réutilisation. [Conditions Wikimedia](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use#7._Licensing_of_Content)

Le dossier `work/` contient le cache et les résultats générés localement.

## Contrôle de l’essai

Les appels en direct à Wikidata et à Wikipédia ont fourni huit révisions pour neuf identifiants recherchés. Le 10 novembre 2026, Spark 4.0.1 a nettoyé les fragments, effectué les deux jointures et relu les résultats Parquet sous Ubuntu WSL. Les sorties contiennent deux synopsis et six introductions biographiques, soit trois par film, avec leur URL de révision. La ligne de Jordan Goldberg, coproducteur crédité sur *Inception*, conserve le statut `no_wikidata_item`.

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

OpenAI Codex a aidé à préparer l’échantillon, écrire la collecte et les transformations Spark, contrôler les URL et les statuts d’absence, et documenter l’exécution. À inclure dans la déclaration du `PROJECT.md` du groupe.
