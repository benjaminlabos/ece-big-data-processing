Le profil des sept fichiers IMDb complets est désormais obtenu : 39 colonnes et
216 477 904 lignes de tables. Les fichiers compressés (2 003 620 065 octets) ont
été vérifiés, et la preuve Windows Docker versionne les taux de valeurs manquantes,
le top 5 et la provenance. Le nettoyage avec contrôles de références tourne
séparément et reste à valider jusqu'à son marqueur de succès.

Le module Spark implémente les huit questions et le top 10 année–genre. Les seuils
restent provisoires et explicitement non validés par les étudiants. Q1 inclut les
films sans note ; Q6 exploite une collecte Wikidata P364 avec provenance,
vocabulaire ISO et mesure de couverture. Aucune langue originale n'est déduite
des titres alternatifs. Aucune collecte de synopsis ou biographies n'est lancée.

Les sorties incluent les Parquet par question, neuf aperçus JSON bornés, le
classement CSV et une sélection de films dédupliquée. Le lanceur Windows enchaîne
préparation, exploration et export des crédits, avec ressources bornées et garde
disque. Le collecteur de preuves refuse les sorties incomplètes et compare les
39 profils aux résultats du profil brut séparé avant publication.

Validation : 38 tests Python contrôlés réussis sous Spark 4.2.0 (77,42 s), contrôle
CLI, réconciliation de 369 502 énoncés de langues et profil complet réel. Les
premières sorties réelles sont publiées : Q1 (13 640 lignes), Q2 (138), Q3 (92),
9 089 places de classement pour 6 257 films distincts. Les cinq autres analyses
attendent la fin du nettoyage de leurs tables. Les critères restent provisoires.
Les branches de Benjamin et REPORT.md restent inchangés.

PR dépendante de #4 ; la cible sera ajustée après les approbations des PR parentes.

Un export ciblé produit les couples tconst,nconst, les détails de rôles et la
couverture des seuls films sélectionnés à partir des tables parentes terminées.
Il conserve les contrôles de qualité en évitant de normaliser les crédits hors
sélection ; un test compare ce filtrage au calcul non filtré, avec Unicode et
clés conflictuelles. Son périmètre ne certifie pas le nettoyage global.

Le pool réel comprend 113 273 couples pour 6 257 films, après réconciliation des
124 499 crédits sélectionnés. Un crédit est rejeté et cinq films ont moins de
trois personnes disponibles. La sélection de travail limite l'entrée de collecte
à trois personnes et deux réserves par film, sans doublon de personne entre rôles.

L'entrée de collecte est maintenant publiée : 18 764 couples prioritaires et
12 466 réserves, soit 31 230 couples pour 19 375 personnes distinctes. Les CSV
à deux colonnes respectent le contrat de la PR #3. La vérification des effectifs
par film, identifiants, unicité, union disjointe et empreintes est versionnée.
Cinq films restent sous trois personnes disponibles. Les seuils et la politique
de sélection restent provisoires. Un brouillon de coordination est fourni pour
Benjamin, sans envoi automatique.

Le retour de Benjamin sur les titres Wikipédia vides après conversion des `_`
est intégré : normalisation identique au lecteur streaming, rejet des titres
vides et test avec ses fonctions exactes. La correction figure aussi dans la
PR #4 (6689549), dont les 28 tests passent.

Après interruption de Docker, la préparation reprend les cinq tables terminées
après vérification des marqueurs, schémas, compteurs et audits. Les fichiers sont
réutilisés par liens physiques ; le profil brut complet est réutilisé après
vérification du manifeste. La comparaison des profils est donc un contrôle de
cohérence, sans prétendre à un second calcul indépendant. Le checkpoint ancien
exige une assertion explicite de son année ; les nouveaux checkpoints tracent
aussi les empreintes des entrées et des règles. Les tests comparent la reprise
au traitement complet des sept tables de fixture et vérifient la conservation
du checkpoint source. La validation du traitement global réel reste attendue.

Un workflow GitHub hébergé prépare une exécution indépendante du PC, bornée à
350 minutes. Il exige le même lot IMDb par empreintes, réutilise seulement le
profil brut mesuré, recalcule les tables, collecte les langues avec provenance
et publie les sorties dérivées après contrôle de tous les marqueurs et des
réconciliations. Les journaux sont conservés même en cas d'échec. La préparation
du workflow ne constitue pas une preuve d'exécution cloud réussie.

Le workflow est désormais publié et l'exécution hébergée a démarré :
https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43087424074
La preuve de lancement précise le runner et le commit. Elle ne certifie pas
encore la réussite du lot complet. Le PC n'est plus requis pour ce job.

La première exécution cloud a réussi les 40 tests puis a refusé la réutilisation
du profil brut : IMDb avait mis à jour name.basics. Le lancement calcule désormais
un nouveau profil complet dès qu'une empreinte source diffère, sans reprendre
les compteurs anciens. La provenance du profil est conservée et vérifiée contre
le manifeste de préparation. Une régression contrôle le recalcul et le rejet
des compteurs périmés. La première exécution est un échec, pas une validation du
lot complet ; la relance doit encore terminer.

Le contrôle d'intégration supplémentaire traverse la chaîne complète sur une
fixture synthétique, y compris les neuf sorties, les exports et leurs empreintes.
Les trois tests cloud ciblés passent (50,67 s), en complément de la suite de
40 tests déjà exécutée ; ces nombres se recouvrent. Le paquet conserve désormais
les catalogues IMDb et movie_catalog ainsi que clean/title.ratings pour le binôme.
Le contrôle du manifeste utilise son empreinte originale du checkpoint plutôt
que l'empreinte d'une copie JSON reformatée. Le brouillon de coordination intègre
la PR #3 actualisée de Benjamin et distingue collecte partielle et collecte finie.

Le traitement nocturne passe maintenant par six jobs hébergés. Trois phases
de préparation publient et restaurent des checkpoints vérifiés, puis l'analyse
utilise le lot complet. La limite de 350 minutes porte sur chaque job de calcul,
pas sur le workflow entier. Les données brutes sont figées une fois par run ;
les références légères sont en artefacts pour rester disponibles malgré
l'éviction des anciens caches lourds. Un checkpoint partiel n'a pas de marqueur
global de réussite. Les 42 tests passent (121,52 s), dont la comparaison des
trois phases avec le traitement continu des sept tables. Les chemins de cache,
les dépendances du workflow et les scripts embarqués ont été contrôlés.

Une finition automatique attend la fin du run amont désigné, sans redémarrer le
calcul actif. Elle recalcule le classement, compare les seuils, prépare les CSV
prioritaires/réserves et mesure les écarts avec les entrées de Benjamin. Les
artefacts sont vérifiés, les critères restent provisoires et les petits résultats
peuvent être ajoutés à cette PR sans merge ni modification des branches de Ben.
Une reprise de l'analyse est bornée et exige la préparation complète.
Les tests ciblés couvrent toute la chaîne, la sensibilité aux seuils, les écarts
d'entrée et les sidecars CRC absents des artefacts GitHub.
