# Démonstration et accès aux résultats

## Conditions de départ

Utiliser un clone neuf du dépôt intégré ; le commit technique main vérifié est
`4781544fbd2282088a2bb51ec156be40cd0fdc3e`. Après les derniers documents et le
rapport, noter aussi le SHA effectivement soumis. `PROJECT.md` décrit
la construction des données depuis les sources. Les étapes ci-dessous montrent
les sorties réelles existantes et les contrôles reproductibles ; les contrôles
synthétiques et le rejeu ne sont pas un nouveau calcul complet ou une capture
Wikimedia vivante.

Sur Windows : Docker Desktop en moteur Linux pour Spark/Kafka. L’audit reçu
utilise un Python 3.12 isolé avec PyArrow 26.0.1 et DuckDB 1.4.3. Les dossiers
de sortie doivent être neufs. Les chemins locaux de cette fiche sont des
emplacements vérifiés de Nicolas, à remplacer pour une autre machine.

## 1. Préparation, exploration et classement

Présenter les profils des 39 colonnes et les contrôles qualité dans
[full-snapshot-43093003953](full-snapshot-43093003953/), puis les comptes des
[tableaux factuels](REPORT-FACTS-2026-11-12.md). Les résultats complets proviennent
du run [43125611999](https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43125611999).

Les neuf CSV intégraux locaux ont les nombres de lignes suivants :

| CSV | Lignes |
| --- | ---: |
| q1 | 13 640 |
| q2 | 138 |
| q3 | 92 |
| q4 | 47 |
| q5 | 535 |
| q6 | 5 826 |
| q7 | 6 158 |
| q8 | 127 468 |
| top10_per_genre_year | 9 089 |

Ces CSV sont dans `/workspace/full-results-csv-43125611999` dans l’environnement
cloud. Le ZIP de livraison Nicolas contient les sorties complètes déjà relues
par Benjamin. Pour régénérer les CSV à partir du paquet vérifié du run, utiliser
un dossier de sortie neuf :

```bash
python scripts/export_verified_query_csv.py --verified-root results/final/verified --output results/demo-query-csv
```

Installer DuckDB 1.4.3 dans l’environnement isolé. Le paquet doit être extrait
vers le chemin fourni ; le script vérifie ses empreintes et les nombres de lignes.
Les fichiers `query-review/` versionnés sont des aperçus de 100 lignes maximum.
Le [guide de génération locale](../deliverables/query-outputs/README.md) conserve les effectifs et les empreintes des neuf exports. Les CSV complets restent locaux.

## 2. Enrichissement final v2

Sur le PC de Nicolas, la copie reçue, extraite et auditée se trouve dans :

```text
C:\Users\nriha\ece-big-data-peer-reviewed\results\received-enrichment-v2-2026-11-13
```

Elle contient `inputs/`, `coverage-v2/`, `cache-final.zip`, `cache-restored.json`
et `RECEIVED-ARCHIVE-AUDIT.json`. L’archive source reste dans les Téléchargements.
Son SHA-256 est
`6a54dbed30efd4be905318e1fe053f0b75718dace5b33980d8bd57e546e23e81`.
Le cache reconstitué a pour SHA-256
`ff1d648c0cb9048779eb72e41afb03e32ea94a31e2e1b6d411856f57be35e576`.

Depuis la racine du dépôt dans PowerShell, après activation du Python isolé :

```powershell
$received = 'C:\Users\nriha\ece-big-data-peer-reviewed\results\received-enrichment-v2-2026-11-13'
python project/enrichment/audit_outputs.py "$received/coverage-v2" `
  "$received/inputs/selected-titles.csv" "$received/inputs/enrichment-input.csv" `
  "$received/inputs/primary.csv" "$received/inputs/top10.csv" `
  --snapshot "$received/cache-restored.json" --receipt "$received/cache-final.receipt.json" `
  --require-complete
```

Ce contrôle a été réellement reproduit sur Nicolas : 25 630 réponses, quatre
tables Parquet et 979 groupes ; 5 174 synopsis, 13 159 biographies distinctes,
5 369 films avec trois biographies, 4 710 ayant aussi un synopsis. Les 1 546
films incomplets et les cinq cas structurels restent visibles. Montrer également
les URL sources, révisions et licences dans les Parquet. La sélection initiale
reste dans les preuves de la PR #3.

Pour un autre poste, extraire le même paquet, vérifier son SHA et reconstituer
le snapshot selon le README de l’archive ; ne pas supposer le chemin Windows
ci-dessus disponible. Le paquet ne contient pas le code : cloner le dépôt.

## 3. Question libre

Présenter [comparisons.csv](collaborations-full-43093003953/comparisons.csv),
[summary.json](collaborations-full-43093003953/summary.json) et les diagnostics.
Les dix comparaisons viennent du lot complet commun, pas du classement top 10.
Au seuil 1 000 votes / deux années : 16 884 binômes, moyennes 6,529691 et
6,181983, différence appariée −0,347708. Les étudiants exposent personnellement
l’interprétation et les limites.

Pour recalculer l’analyse, suivre
[le guide du composant](../project/collaborations/README.md). Il faut les quatre
Parquet nettoyés et le manifeste du même lot ; le ZIP d’enrichissement ne les
contient pas. Leur acquisition et leur empreinte doivent être vérifiées avant
un nouveau calcul. La présentation des dix résultats déjà audités n’exige pas
ce nouveau traitement.

## 4. Streaming

Vérification rapide hors ligne depuis la racine :

```bash
python validation/check_demo_payload.py
```

Elle vérifie les empreintes du jeu fourni, les 1 845 identifiants uniques,
1 311 modifications admissibles, sept correspondances et six pages.
Le reçu est `results/demo-payload-audit.json`. Ce contrôle ne démarre pas Kafka.

Pour démontrer réellement le trajet Kafka/Spark, utiliser le rejeu décrit dans
[PROJECT_STREAMING.md](../PROJECT_STREAMING.md#rejeu-de-démonstration-depuis-un-clone).
Choisir une sortie neuve dans le conteneur, par exemple `/tmp/gr04-demo-final-v1`,
à la place de `/tmp/gr04-demo` si un précédent rejeu existe. Le rejeu traite
la capture du 11 novembre et crée un topic/checkpoint propres ; il ne constitue
pas une nouvelle capture vivante. Les métriques de latence et la capture de
15 minutes sont dans les preuves Docker existantes.

## 5. Contrôle final du clone

Les lanceurs `validation/run_cross_component_checks.ps1` (Docker) et `.sh`
(Linux natif) vérifient les contrats sur un jeu synthétique dans un clone neuf.
Voir [INTEGRATION-CANDIDATE.md](INTEGRATION-CANDIDATE.md). Ces contrats ont déjà
été contrôlés ; rejouer sur le commit final après intégration et conserver le
SHA et les reçus. Un clone propre doit aussi contenir REPORT.md terminé,
la déclaration d’usage de l’IA et les liens accessibles des sorties réelles.
