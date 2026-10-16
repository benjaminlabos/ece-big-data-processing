# Lab 3 — Structured Streaming / Wikimedia (séance 05)

Le notebook reprend la démo du cours et réalise les explorations demandées :

- nombre de modifications par robot / humain sur une heure ;
- fenêtres fixes de **1 minute**, avec nombre de modifications et tailles en octets ;
- fenêtres glissantes de **2 minutes**, décalées de **30 secondes**, par page ;
- filtres du producteur : langue, titre de page et exclusion des robots ;
- temps d'événement, watermark de 2 minutes et exports CSV des agrégations.

[Consignes du cours](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/05.structured-streaming/lab_docker_wikistreams.md).
Les résultats présents dans le notebook sont ceux de son exécution réelle.

Validation du 16 octobre 2026 : **29 modifications réelles distinctes**, dont 5
signalées comme robots et 24 autres ; **11 cellules de code exécutées sans erreur**.
Les fenêtres fixes comptent 29 événements et les fenêtres glissantes 116 appartenances
(4 par événement). Les comptes et toutes les mesures de taille des CSV ont été
recalculés indépendamment depuis les événements capturés et concordent. Les
identifiants Wikimedia `meta.id` ont aussi été vérifiés pour exclure les doublons.

## Lancement sous PowerShell

Docker Desktop doit être ouvert. Depuis la racine du dépôt :

```powershell
cd 05.structured-streaming\lab_docker_wikistreams
docker compose up -d --build
docker compose exec pyspark-notebook python /home/jovyan/admin.py
docker compose exec pyspark-notebook python /home/jovyan/wikistream_producer.py
```

Lancer **un seul producteur par topic** pour éviter deux envois du même événement.
Le producteur suit Wikipédia en français pendant **10 minutes**. Le laisser
tourner ; ouvrir un **deuxième terminal**, dans le même dossier :

```powershell
docker compose exec pyspark-notebook jupyter server list
```

Ouvrir le lien affiché en remplaçant son port `8888` par **`8889`** et son
adresse par **`localhost`**, en conservant le token. Ouvrir
`work/wikistream_pyspark.ipynb` et choisir **Run → Run All Cells**.
Le notebook observe le flux pendant 45 secondes, affiche les résultats,
exporte les CSV, puis arrête ses trois requêtes. Le premier lancement peut
prendre quelques minutes pour télécharger le connecteur Spark/Kafka.

```powershell
docker compose logs -f pyspark-notebook
```

La sortie de la démo est visible dans le notebook. Pour les deux explorations,
un échantillon des résultats de chaque micro-lot est aussi écrit dans les logs
Docker : la commande ci-dessus permet donc de suivre les résultats du consommateur,
comme demandé dans le lab. Nous utilisons `docker compose logs` plutôt qu'un nom
de conteneur fixe pour éviter un conflit avec les conteneurs des anciens TP.
Pour relancer et enregistrer le notebook depuis un terminal :

```powershell
docker compose exec -T pyspark-notebook jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=300 /home/jovyan/work/wikistream_pyspark.ipynb
```

## Exemples de filtres

```powershell
# Observe French and English Wikipedia for 2 minutes.
docker compose exec pyspark-notebook python /home/jovyan/wikistream_producer.py --languages fr,en --duration 120

# Observe one exact French page title, excluding bots.
docker compose exec pyspark-notebook python /home/jovyan/wikistream_producer.py --languages fr --page "Paris" --exclude-bots --duration 120
```

Les filtres s'appliquent **avant l'envoi à Kafka** ; un titre peu modifié peut
ne produire aucun événement. Les messages déjà présents dans le topic ne sont
pas effacés lorsqu'on change de filtre. Pour isoler une exploration, utiliser
un nouveau topic avec `--topic` dans `admin.py` et dans le producteur, puis
définir `WIKISTREAM_TOPIC` pour l'exécution du notebook.
Choisir aussi un fichier local distinct avec `--output` pour cette nouvelle capture :
le producteur utilise les identifiants déjà présents dans ce fichier pour éviter
de renvoyer les mêmes événements après une reconnexion ou une relance.

## Fichiers et résultats

- `notebooks/wikistream_pyspark.ipynb` : démo, explorations et résultats exécutés.
- `admin.py` : création et vérification du topic `wikistreams` à une partition.
- `wikistream_producer.py` : capture Wikimedia, filtrage, envoi et confirmation Kafka.
- `data/results/tumbling_1min.csv` : agrégations par minute, wiki et catégorie de bot.
- `data/results/sliding_2min_30s.csv` : agrégations glissantes par page.
- `data/results/validation.json` : compteurs de l'exécution Spark.
- `data/wiki_events.jsonl` : copie locale des événements envoyés, ignorée par Git.
- `jobs/` : ressources complémentaires pour Spark.

Les CSV sont remplacés au fil des micro-lots et ne sont pas un sink transactionnel.
Un nouveau checkpoint est créé à chaque exécution pour permettre de rejouer la
démo depuis les premiers messages encore disponibles. Le watermark peut exclure
des événements anciens lorsque l'historique du topic dépasse son retard autorisé.
La somme des comptes glissants est quatre fois celle des événements retenus,
car chaque événement appartient à quatre fenêtres.

## Ports et arrêt

Jupyter : **localhost:8889** ; Spark UI : **localhost:4045** pendant l'exécution ;
Kafka depuis Windows : **localhost:19092** ; Kafka depuis les conteneurs :
**kafka:29092**. L'environnement du TP04 peut rester ouvert.
Les bibliothèques du producteur sont installées dans Docker ; aucune installation
Python sur Windows n'est nécessaire. L'image Jupyter est fixée par son digest et
le notebook sélectionne un connecteur correspondant à ses versions Spark/Scala.

```powershell
# Stop the lab while retaining its containers and Kafka messages.
docker compose stop

# Start it again.
docker compose start
```

`docker compose down` supprime les conteneurs et donc les messages Kafka du lab.
Les notebooks et résultats dans les dossiers partagés restent sur le PC.
