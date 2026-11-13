> Exécution terminée : les sept PR de ce plan ont été fusionnées dans main
> `4781544fbd2282088a2bb51ec156be40cd0fdc3e`. La nouvelle tête #2 `a5584e9`
> a reçu l’approbation 5889316722 avant fusion. Le plan ci-dessous conserve
> l’ordre et les précautions utilisés pour l’intégration.

# Ordre d’intégration préparé — 13 novembre 2026

Ce plan est une répétition locale, pas une fusion sur GitHub. Un seul membre
exécute les fusions. Les votes restent personnels ; vérifier les têtes et les
approbations immédiatement avant chaque opération. L’accord de fusion du
binôme et les décisions de paramètres restent à enregistrer.

## Chaîne de dépendances

| Ordre proposé | PR | Tête répétée | Cible actuelle | Action avant fusion |
| --- | --- | --- | --- | --- |
| 1 | #1 | 630fd4d | main | Vérifier l’approbation de Benjamin |
| 2 | #4 | 6689549 | feat/imdb-data-preparation | Après #1, recibler vers main et vérifier le diff et le vote de Benjamin |
| 3 | #5 | 7790935 | feat/imdb-integration-exports | Après #4, recibler vers main et vérifier le diff et le vote de Benjamin |
| 4 | #8 | 0746647 | feat/imdb-full-analysis | Après #5, recibler vers main et vérifier le diff et le vote de Benjamin |
| 5 | #2 | e6e5933 | main | Résoudre le conflit du guide, puis faire relire la nouvelle tête |
| 6 | #3 | 409f167 | main | Vérifier l’approbation Nicolas sur cette tête |
| 7 | #6 | 4bfbf2a | main | Vérifier l’approbation Nicolas sur cette tête |

Conserver les branches parentes tant que leurs PR dépendantes n’ont pas été
reciblées. Utiliser des merge commits pour garder l’ascendance des branches
empilées. Ne pas pousser directement sur main. Après chaque changement de
base, vérifier que le diff ne réintroduit pas les tâches parentes.

## Conflit connu de la PR #2

La répétition partant de main `1431275` rencontre uniquement un conflit add/add
sur `PROJECT.md`. Conserver le guide de préparation dans `PROJECT.md`, le guide
streaming complet à la racine dans `PROJECT_STREAMING.md`, puis la navigation
vers les composants. Garder les deux fichiers au même niveau préserve les
chemins relatifs. La résolution du candidat local fournit une référence.

Le merge simulé ne dispense pas de mettre à jour la branche de la PR #2 :
intégrer le main courant dans sa branche, résoudre le conflit et publier cette
résolution avant la fusion via PR. Nicolas doit vérifier et approuver la nouvelle
tête ; le vote sur e6e5933 ne couvre pas automatiquement la résolution publiée.
Si Benjamin constate d’autres changements, suspendre la fusion et les relire.

## Répétition effectuée

Le reçu [merge-rehearsal.json](peer-review-2026-11-13/merge-rehearsal.json)
contient les sept SHA complets, l’ordre et le conflit. Le code de préparation,
streaming, enrichissement, question libre, scripts et tests est identique au
candidat audité `6a5d4a1`. Les nouveaux documents de clôture sont à faire relire
séparément avant publication ; ils ne changent pas les têtes #4/#5/#8 attendues
par Benjamin.

## Après les fusions

- Ajouter les documents de clôture via une PR de documentation relue par l’autre
  membre ; éviter les copies de fichiers écrasant le travail de Benjamin.
- Terminer le rapport personnel et ses liens vers les sorties complètes.
- Cloner main dans un dossier neuf, noter le SHA exact, vérifier les dépendances,
  les chemins d’artefacts et les commandes de démonstration.
- Exécuter les contrôles appropriés au résultat intégré et conserver leurs reçus.
- Vérifier l’accès du correcteur aux sorties ; les seuls chemins locaux Windows
  et les aperçus de 100 lignes ne constituent pas les sorties intégrales.
- Les étudiants effectuent l’envoi de soumission après vérification de REPORT.md.
