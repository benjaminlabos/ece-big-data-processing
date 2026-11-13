# Tableaux factuels pour le rapport personnel

Sources et résultats vérifiés ; les explications méthodologiques, alternatives
et interprétations sont à rédiger personnellement par les étudiants. Les
chiffres d'enrichissement ci-dessous correspondent à la sélection initiale finale publiée le 12 novembre ; la projection de la PR #8 est indiquée séparément.

## Préparation du lot complet

Source : run 43093003953 ; finition réussie 43125611999.

| Table | Lignes brutes | Lignes propres | Quarantaine | Doublons normalisés retirés |
| --- | ---: | ---: | ---: | ---: |
| title.basics | 12952978 | 12952978 | 0 | 0 |
| name.basics | 15806985 | 15806985 | 0 | 0 |
| title.akas | 60328927 | 60328862 | 65 | 0 |
| title.crew | 12952048 | 12952010 | 38 | 0 |
| title.episode | 10026073 | 10026073 | 0 | 0 |
| title.principals | 102704599 | 102704599 | 0 | 0 |
| title.ratings | 1731223 | 1731221 | 2 | 0 |

Total : 216 502 833 lignes brutes ; 216 502 728 propres ; 105 en quarantaine.
Les mises à null et retraits de références ont leurs comptes séparés dans
`validation/full-snapshot-43093003953/completed-anomaly-counts.csv`.

| Colonne | Valeurs manquantes (%) |
| --- | ---: |
| title.akas.attributes | 99.474224 |
| title.basics.endYear | 98.731018 |
| name.basics.deathYear | 98.320742 |
| name.basics.birthYear | 95.661842 |
| title.principals.job | 80.661539 |

## Exploration et classement

Source : validation/overnight-43093003953/query-review/QUERY_REVIEW.json.
The query-review CSV files contain previews of up to 100 rows. See the
[query export guide](../deliverables/query-outputs/README.md) for the verified
Parquet artifact and local generation of the complete CSV files.

| Sortie | Lignes complètes |
| --- | ---: |
| q1 | 13640 |
| q2 | 138 |
| q3 | 92 |
| q4 | 47 |
| q5 | 535 |
| q6 | 5826 |
| q7 | 6158 |
| q8 | 127468 |
| top10_per_genre_year | 9089 |

Classement : 6 256 films distincts, 9 089 places dans 979 groupes année–genre.
Paramètres calculés, encore marqués provisoires : durée minimale 40 minutes,
1 000 votes, années 1980–2025 ; z-score de durée 2,0 ; succès 7,0 ; fraction
de début de carrière 0,25 ; minimum de votes par épisode 10.

## Enrichissement : collecte finale, sélection initiale

| Mesure | Valeur |
| --- | ---: |
| Identifiants traités / attendus | 25 630 / 25 630 |
| Identifiants en attente / fichiers endommagés signalés | 0 / 0 |
| Absences documentées avant extraction | 7 269 |
| Textes indisponibles après extraction | 7 297 |
| Films sélectionnés | 6 256 |
| Personnes demandées | 19 374 |
| Couples initiaux | 31 225 |
| Synopsis disponibles | 5 174 |
| Biographies distinctes disponibles | 13 159 |
| Films avec trois biographies | 5 229 |
| Films avec aussi un synopsis | 4 608 |
| Films atteignant trois biographies grâce aux réserves | 970 |
| Films incomplets | 1 648 |
| Films sous trois candidats | 5 |

Source : `project/enrichment/evidence/final-enrichment-2026-11-12.json`,
commit de la PR #3 `9fcef57`. Vérification indépendante du 13 novembre :
empreintes des deux CSV publiés et reconstruction exacte des 979 groupes depuis
le classement commun et les statuts des films incomplets. La vérification initiale ne disposait pas du cache ni des Parquet.
Leur copie reçue par Nicolas a ensuite été contrôlée, voir le bilan de réception ci-dessous.

## Enrichissement v2 : exécution finale publiée

Spark 4.2.0 a traité les entrées exactes de #8 (`0746647`), soit 32 285 couples
incluant les 1 060 compléments, sur le même cache final. Source : manifeste
`project/enrichment/evidence/final-enrichment-v2-2026-11-13.json`, commit #3
`409f167`. La sélection initiale ci-dessus reste disponible.

| Mesure | V2 | Différence avec la sélection initiale |
| --- | ---: | ---: |
| Synopsis | 5 174 | 0 |
| Biographies distinctes | 13 159 | 0 |
| Films avec au moins trois biographies | 5 369 | +140 |
| Films avec aussi un synopsis | 4 710 | +102 |
| Films incomplets | 1 546 | -102 |
| Films sous trois candidats | 5 | 0 |

Films incomplets : 659 sans synopsis seulement, 464 sans trois biographies
seulement et 423 manquant des deux. Les 970 franchissements par réserves
initiales et les 140 par compléments v2 donnent 1 110 franchissements non
prioritaires. Aucun nouvel identifiant et aucune nouvelle collecte.

Contrôle indépendant ici : trois empreintes CSV publiées, huit empreintes des
entrées exactes, reconstruction des 979 groupes et des 140 franchissements
(dont 102 avec synopsis) depuis les deux listes de films incomplets et les
couples supplémentaires.

Réception vérifiée ensuite sur le PC Nicolas : 41 entrées du manifeste et 17
fichiers Parquet passent leurs empreintes. Le cache de 25 630 réponses a été
reconstitué à l’identique ; `audit_outputs.py --require-complete` avec PyArrow
26.0.1 reproduit exactement le bilan publié. DuckDB 1.4.3 recalcule
indépendamment 5 369 / 4 710 films, +140 / +102 compléments et 970
franchissements par réserves initiales. Les 979 groupes sont vérifiés.
Reçu : `validation/peer-review-2026-11-13/received-v2-archive-audit.json`.
Archive de reproduction annoncée : `GR04-enrichissement-v2-0746647.zip`,
93 672 306 octets, SHA-256
`6a54dbed30efd4be905318e1fe053f0b75718dace5b33980d8bd57e546e23e81`.

La proposition alternative de 342 couples (+220 / +152) et les gains +417 / +62
sur l'instantané historique sont distincts du résultat v2 ci-dessus.

## Streaming : capture Docker réelle

| Mesure | Valeur |
| --- | ---: |
| Durée de capture avec vidage | 15 min 25,948 s |
| Événements livrés et relus | 1 845 |
| Modifications admissibles | 1 311 |
| Correspondances | 7 |
| Pages rapprochées | 6 |
| Médiane modification–Spark | 20,165 s |
| 95e percentile modification–Spark | 78,908 s |

Source : project/streaming/evidence/docker-live-2026-11-11.json. Les preuves
de rejeu, de reprise et de watermark ont leurs fichiers distincts ; citer le
périmètre de chaque mesure. Les présents contrôles hors ligne ne sont pas une
nouvelle capture vivante.

## Question libre : lot complet commun

| Mesure au seuil 1 000 votes / deux années | Valeur |
| --- | ---: |
| Binômes | 16884 |
| Films de première année par binôme, somme | 17158 |
| Films ultérieurs par binôme, somme | 23023 |
| Moyenne première année, poids identique par binôme | 6.5296913251204325 |
| Moyenne années suivantes, poids identique par binôme | 6.18198301883445 |
| Différence appariée moyenne | -0.3477083062859799 |

Les dix cellules et les diagnostics sont dans
`validation/collaborations-full-43093003953/`. Source : run 43128551838, code
Benjamin 8a0d75b, Spark 4.2.0, contrôle indépendant DuckDB 1.4.3.

## Éléments personnels exigés par les consignes

- Préparation : méthodologie, raisons, alternatives et arbitrages.
- Exploration : définitions retenues, méthodologie, alternatives et interprétation.
- Enrichissement : sélection, source, absences et couverture finale.
- Streaming : méthode, filtrage et traitement des événements tardifs.
- Question libre : intérêt, méthode, alternatives et interprétation des résultats.

Les étudiants rédigent ces éléments eux-mêmes dans REPORT.md ; les tableaux
ci-dessus servent de références factuelles.
