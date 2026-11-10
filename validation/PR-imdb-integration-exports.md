Les résultats préparés peuvent maintenant alimenter les interfaces des PR
streaming et enrichissement : un catalogue d'identifiants IMDb en Parquet, un
catalogue de films 1980–2025 avec notes/votes, un CSV de crédits `tconst,nconst`
pour une sélection externe, et un CSV Wikipédia filtré par les identifiants
nettoyés. Aucun classement de films ou appel Wikimedia n'est ajouté.

Les exports conservent un audit des sélections, crédits et références écartés,
signalent les films ayant moins de trois candidats et réconcilient les lignes.
Les CSV sont de vrais fichiers UTF-8, compatibles avec les consommateurs de
Benjamin ; les gros catalogues restent des Parquet distribués.

Validation Linux : 13 tests réussis, exécutions contrôlée et réelle, Ruff et
contrôle Git réussis. L'essai réel exporte 2 000 identifiants et 14 des 32 liens
du catalogue pilote. L'extrait ne contient aucun film de 1980–2025 : les deux films
du pilote d'enrichissement sont explicitement signalés absents, sans candidats
fabriqués. Le lecteur streaming exact et le schéma d'enrichissement ont relu les
CSV sous Spark 4.2.0. Les preuves agrégées figurent dans `validation/`.

Cette PR dépend de la PR #1 et vise sa branche pour montrer uniquement les
exports. Après approbation et fusion de #1, la cible pourra devenir `main`.
À relire par Benjamin : contrats CSV/Parquet, couverture des crédits et traitements
des références hors échantillon. L'utilisation commune reste à confirmer.
La sélection top 10 et le traitement complet IMDb restent hors du périmètre de
cette PR. Les textes du rapport reviennent aux étudiants.
