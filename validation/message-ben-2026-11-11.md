Brouillon de coordination Nicolas → Benjamin, actualisé le 11 novembre 2026.
À transmettre par Nicolas ; aucun message n'a été envoyé automatiquement.

Salut Ben,

J'ai pris en compte ta PR #3 au commit 368fe2c. Ton pilote Docker de 13 films
et 48 personnes produit 6 synopsis, 33 biographies et 7 films avec trois biographies.
La reprise conserve bien les 21 réponses déjà en cache d'après ta preuve publiée.
Ton instantané Spark couvre nos 6 257 films, 31 230 crédits et 979 groupes.
Il annonce 33 synopsis disponibles, 156 biographies et 41 films avec trois
biographies ; collection_complete reste faux. Ces compteurs décrivent cet
instantané publié, pas nécessairement l'état courant de ton collecteur.

De notre côté, les sept fichiers sources, le premier profil complet et les
sorties Q1–Q3/classement sont obtenus. Le nettoyage global et Q4–Q8 ne sont pas
encore certifiés. Le traitement est passé sur GitHub Actions pour ne plus dépendre
du PC. La première exécution a refusé le profil ancien après une mise à jour
IMDb ; la nouvelle version recalcule les 39 colonnes du nouveau lot au lieu de
mélanger les compteurs. Les fichiers provisoires que tu utilises restent conservés.
PR #5 : https://github.com/benjaminlabos/ece-big-data-processing/pull/5

Notre prochaine livraison doit fournir les résultats des huit questions, le
classement, les crédits et leur couverture, plus imdb_ids, movie_catalog et
clean/title.ratings. Avant de remplacer tes entrées, nous comparerons les
identifiants du nouveau lot avec ceux du commit 0f82d83 que tu utilises : seuls
les changements utiles seront à collecter. Garde le cache existant. Les couples
prioritaires et réserves restent distincts du pool complet de crédits.

Pour les critères, 40 minutes et 1 000 votes restent provisoires. Il nous reste
à comparer les effectifs année–genre et la couverture, choisir ensemble la
politique, puis figer les exports. La présence de trois candidats ne prouve
pas que trois biographies existent ; tes mesures de couverture sont essentielles.
Les cinq films sous trois personnes doivent garder un statut explicite.

Tu peux continuer sans attendre le dernier calcul de Nicolas :
1. Poursuivre la collecte reprenable et ses instantanés Spark sur les entrées déjà
   validées, avec les réserves, les URL de révision et les motifs d'absence.
2. Finaliser l'intégration du streaming avec les identifiants IMDb et la référence
   wiki,title,imdb_id,wikidata_id ; préserver les preuves réel/rejeu/watermark,
   et valider l'exécution intégrée sous Docker. Le pilote enrichissement Docker
   réussi ne vaut pas encore validation Docker de toute la chaîne streaming.
3. Confirmer et développer la question libre proposée. Les résultats et la
   disponibilité de ses entrées doivent être explicités ; ne pas attendre la
   fusion pour préparer la logique et les contrôles sur un petit jeu.

Ta PR #1 a une approbation formelle, merci. #4 contient le correctif de titres
vides après normalisation (6689549) et attend encore la relecture finale. #5
reste en brouillon tant que les résultats complets ne sont pas contrôlés.
Nous relirons aussi tes PR #2 et #3 avant intégration, sans modifier tes branches.

Après nos livraisons, il restera la validation des critères, la mise en commun
des PR approuvées, l'harmonisation de PROJECT.md, un essai complet reproductible,
la question libre et le rapport rédigé personnellement par les étudiants.
Le lancement cloud n'est pas une preuve de réussite du lot complet.
