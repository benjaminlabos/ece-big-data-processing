# Extension bornée des crédits déjà en cache

Choix retenu côté Nicolas : conserver les catégories IMDb déjà admises, y compris
`self`, `archive_footage` et `archive_sound`, et ajouter au plus trois personnes
déjà disponibles en cache par film. Les trois prioritaires et les deux réserves
initiales sont conservés. La confirmation du binôme est encore attendue.

Cette entrée versionnée ne modifie pas le classement ni les fichiers de la
sélection initiale. Le collecteur actif peut terminer avec ses entrées actuelles :
les ensembles d'identifiants à collecter sont identiques.

| Entrées | Avant | Extension |
| --- | ---: | ---: |
| Films | 6 256 | 6 256 |
| Personnes distinctes | 19 374 | 19 374 |
| Identifiants à collecter (films + personnes) | 25 630 | 25 630 |
| Couples prioritaires | 18 761 | 18 761 |
| Réserves initiales | 12 464 | 12 464 |
| Couples supplémentaires déjà en cache | 0 | 1 060 |
| Couples totaux | 31 225 | 32 285 |

## Fichiers à utiliser après confirmation du binôme

Le dossier `inputs/` contient `selected-titles.csv`, `top10.csv`, `primary.csv`,
`enrichment-input.csv` et `reserves.csv`. Ce dernier réunit les réserves initiales
et les nouveaux compléments. `initial-reserves.csv` et `supplementary.csv`
permettent de les distinguer. La provenance des 1 060 ajouts est conservée dans
`supplementary-provenance.csv`. `POLICY.json` enregistre les comptes, empreintes,
limites et l'état de validation ; `student_validated` reste faux.

Sur le même instantané de 13 782 réponses, la proposition auditée passe de
3 488 à 3 905 films avec trois biographies, et de 2 853 à 2 915 films ayant aussi
un synopsis. Ce sont les gains de cet instantané, pas une nouvelle exécution
Spark sur la collecte finale. Les textes réels du cache de Benjamin ne sont pas
présents dans ce dépôt et n'ont pas été rescannés par ce script.

La proposition provient de la PR #3 au commit
`4ad5567`. Ses couples ont été rapprochés des crédits du run IMDb commun
43093003953. Les catégories et les crédits sont visibles dans `proposal.csv` ;
les preuves et la répartition sont dans `proposal.json`.

## Reconstruire les entrées

Après extraction de l'artefact vérifié du run 43125611999 vers un dossier neuf :

```bash
python scripts/expand_cached_credits.py \
  --baseline validation/overnight-43093003953 \
  --verified results/final/verified \
  --proposal validation/cached-credit-extension-13782/proposal.csv \
  --evidence validation/cached-credit-extension-13782/proposal.json \
  --output results/cached-credit-extension-v2
```

Le programme vérifie les empreintes, le rattachement au lot complet, l'appartenance
au pool de crédits, l'absence de nouveaux identifiants, les doublons et la limite
de trois ajouts. Il accepte les fins de ligne LF/CRLF sans réécrire les sources.
Il refuse un dossier de sortie existant et publie le résultat après les contrôles.
Aucun appel Wikimedia n'est effectué.

## Traitement Spark à faire côté Benjamin

Après la fin du collecteur actuel et l'accord du binôme, figer et sauvegarder un
nouvel instantané selon le guide de la PR #3. Depuis `project/enrichment`, utiliser
les nouveaux CSV avec les scripts existants, dans un dossier de sortie neuf :

```powershell
# $inputs et $snapshot désignent les chemins visibles par Spark dans le conteneur.
spark-submit enrich.py --titles "$inputs/selected-titles.csv" `
  --candidates "$inputs/enrichment-input.csv" --primary "$inputs/primary.csv" `
  --top "$inputs/top10.csv" --raw $snapshot --output work/coverage-cached-v2
```

Relire les quatre Parquet avec `audit_outputs.py`, l'instantané figé et son reçu.
Utiliser `--require-complete` pour une livraison annoncée comme complète. Le champ
historique `films_reaching_three_with_reserves` inclut alors tous les compléments
non prioritaires : expliquer la version de politique et conserver la distinction
entre réserves initiales et compléments du cache dans les preuves.

Le binôme garde les deux couvertures (initiale et étendue) si elles sont comparées.
Les absences et les cinq films sous trois candidats restent signalés. Les choix
méthodologiques et leurs raisons sont rédigés personnellement dans REPORT.md.
