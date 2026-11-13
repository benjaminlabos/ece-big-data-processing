# Génération locale des sorties des requêtes

Les neuf CSV de Q1 à Q8 et du classement sont générés localement. Le dépôt
fournit le manifeste de vérification (effectifs, SHA-256 et colonnes JSON).
Les exports complets contenant des champs IMDb sont exclus de la livraison
publique. Consulter les [conditions IMDb](https://help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/G5JTRESSHJBBHTGX).

Information courtesy of IMDb (https://www.imdb.com). Used with permission.

Depuis la racine du dépôt, avec Python et DuckDB 1.4.3, pointer vers le paquet
Parquet vérifié du run 43125611999, déjà extrait localement. Utiliser un dossier
de sortie neuf et comparer les effectifs et empreintes au manifeste joint.

### Retrieve the verified input

Open the [GitHub Actions run](https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43125611999)
and sign in to GitHub to access its Artifacts section. Where access and source
terms permit, download `imdb-completed-for-review-43125611999-1` (artifact
13904091705). GitHub lists its expiry as 10 February 2027. Extract the archive
and locate `VERIFIED.json`. Use its containing directory as `--verified-root`;
the command below assumes that directory is `results/final/verified`.

The manifest and bounded previews linked from the [demonstration guide](../../validation/DEMONSTRATION-2026-11-13.md)
remain available in Git. If the archive is unavailable, follow the full-run
instructions in [PROJECT.md](../../PROJECT.md) with official inputs. A fresh
IMDb snapshot may yield different results and checksums.

```powershell
python -m pip install duckdb==1.4.3
python scripts/export_verified_query_csv.py --verified-root results/final/verified --output results/demo-query-csv
```

Pour repartir des jeux de données IMDb officiels, suivre [PROJECT.md](../../PROJECT.md).
Les empreintes du manifeste correspondent à l'instantané historique de ce run.
