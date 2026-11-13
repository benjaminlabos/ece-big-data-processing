# Candidat d’intégration local — gr-04

Le dossier /workspace/gr04-integration-candidate rassemble les commits publiés
par Nicolas et Benjamin pour tester leurs interfaces. Il n'est pas publié sur
main et ne constitue pas une approbation des PR.

La simulation de fusion a trouvé un conflit add/add sur PROJECT.md.
La résolution locale garde le guide de préparation à la racine, conserve
intégralement le guide streaming dans PROJECT_STREAMING.md (même niveau pour
préserver ses chemins relatifs), et ajoute les liens vers l'enrichissement et la
question libre. Les fusions des deux autres composants n'ont pas de conflit.
Les commits originaux sont conservés par les merges locaux.

Le test check_cross_pipeline.py utilise les sorties réelles du code de
préparation sur des données synthétiques, puis leurs CSV et Parquet directement
dans les lecteurs de Benjamin. Il vérifie notamment les titres Wikipédia vides,
les identifiants absents, la couverture d'enrichissement et la distinction entre
cache complet et trois biographies disponibles. Aucun appel Wikimedia n'est
fait ; les textes synthétiques sont explicitement limités au test.

Le candidat doit être relu et testé sur les données et livrables finaux après
les approbations croisées. Aucun résultat synthétique ne valide le calcul sur
IMDb complet. Le rapport des étudiants reste à rédiger personnellement.

## Reproduire les contrôles depuis un clone neuf

Ces commandes s'appliquent au candidat local, après import du bundle dans le
clone Windows existant ; cette branche n'est pas publiée sur GitHub.

```powershell
git clone --no-local C:\Users\nriha\ece-big-data-peer-reviewed C:\Users\nriha\gr04-clean-check
Set-Location C:\Users\nriha\gr04-clean-check
# Si l'image du TP n'existe pas encore :
docker compose -f 05.structured-streaming/lab_docker_wikistreams/compose.yaml build pyspark-notebook
.\validation\run_cross_component_checks.ps1
```

Le lanceur ne démarre pas Kafka et n'appelle pas Wikimedia. Il utilise deux CPU,
2500 Mo de mémoire et les fichiers suivis par Git. Il refuse d'écraser un résultat
existant. Le premier contrôle produit les données synthétiques, les sept tables,
les huit analyses et les sorties consommées par Benjamin. Le deuxième relit
les résultats et teste les refus de corruption. Les reçus sont dans `results/`.
La commande de clonage réseau du guide principal vise `main` après approbations
et fusions ; elle ne pointe plus vers la branche de préparation seule.

## Contrôle natif Linux depuis un clone neuf

Java 21 et Python 3.10 ou ultérieur sont requis. Les dépendances sont installées
dans un environnement isolé. Le lanceur transmet les chemins d'import aux
workers Spark et refuse d'écraser des sorties existantes.

```bash
python -m venv .venv-review
source .venv-review/bin/activate
python -m pip install -r requirements-preparation.txt -r project/enrichment/requirements.txt duckdb==1.4.3
GR04_PYTHON="$(command -v python)" bash validation/run_cross_component_checks.sh
```

Le clone source 63ef1b75d9b8525c8e94b7779ddd95f1796f0ff7 a passé les contrôles
le 12 novembre. Le contrôle supplémentaire de la question libre et les preuves
sont dans validation/peer-review-2026-11-12/. Aucune fusion main ni approbation
GitHub n'est produite par ces scripts.


## Préparation de clôture du 13 novembre

Les têtes publiées ont été fusionnées dans une répétition isolée depuis le
main initial. L’ordre et la résolution du seul conflit PROJECT.md sont dans
[MERGE-PLAN-2026-11-13.md](MERGE-PLAN-2026-11-13.md). Le code obtenu est
identique au candidat audité ; cette répétition ne modifie pas GitHub main.
Le guide [DEMONSTRATION-2026-11-13.md](DEMONSTRATION-2026-11-13.md) distingue
les commandes du clone, les artefacts locaux vérifiés et les résultats complets.
Le rapport personnel est préparé sous forme de trame uniquement.
