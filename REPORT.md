# ECE Big Data Processing — Group gr-04

Nicolas Rihana (nicohris) and Benjamin Labos (benjaminlabos)

## Project overview

We cleaned seven IMDb files with PySpark. We used them for the eight exploration questions, film enrichment, and our question about directors and performers. We also used Kafka and Spark Structured Streaming to find Wikipedia edits linked to IMDb titles.

We kept the three parts separate. We can rerun calculations from Parquet, resume enrichment from the cache, and replay a streaming capture. The manifests record the inputs, settings, and outputs for each run.

## IMDb data preparation

We processed 216,502,833 rows across seven tables. We checked [39 columns](validation/full-snapshot-43093003953/missing-profile.csv) for missing values. The column "title.akas.attributes" had the most missing values, at 99.47%. Here are the [five columns with the highest rates](validation/full-snapshot-43093003953/top5-missing.csv).

| Column | Missing values |
| --- | ---: |
| title.akas.attributes | 99.47% |
| title.basics.endYear | 98.73% |
| name.basics.deathYear | 98.32% |
| name.basics.birthYear | 95.66% |
| title.principals.job | 80.66% |

For 28 people, the death year was earlier than the birth year. We set "deathYear" to null and kept the records. We could still find their credits through "nconst". A value of 0 could be treated as a real year in later calculations.

Two rating rows had title IDs missing from the cleaned catalogue. We quarantined them and recorded why. In all, we quarantined 105 rows. We counted field corrections separately because those rows stayed in the dataset.

## IMDb exploration

### Q1 — Films with unusual runtimes

We calculated the average runtime and standard deviation for each genre. We kept feature films at least two standard deviations from their genre average. For a standard deviation of 10 minutes, the threshold is 20 minutes. For 25 minutes, it is 50 minutes. We got 13,640 rows.

### Q2 and Q3 — Most popular genres

For each year from 1980 to 2025, we added up film votes by genre and kept the top three. A genre with many films can rank high even if each film gets fewer votes. In Q3, we compared the top three with the other genres. We used total votes, the number of film–genre pairs, and the average rating. A film with several genres can appear in both groups. We got 138 rows for Q2 and [92 for Q3](validation/overnight-43093003953/query-review/q3-preview.csv).

In 1980, the top three had 7,551,475 votes across 244 film–genre pairs. Their average rating was 6.22. The other 19 genres had 9,436,537 votes across 441 pairs. Their average was 6.30.

### Q4 — Directors with one highly rated film

We looked for directors with exactly one film rated 8 or higher and at least two other films. Those other films had to average 6 or less. An individual film can go above 6 if the average stays within the limit. We got 47 rows.

### Q5 — Best episode near the start and worst episode at the end

We kept series and miniseries with at least three seasons of ten episodes or more. We counted all episodes for season length. We used only episodes with at least ten votes to compare ratings. The best episode had to be in the first two seasons. The worst had to be in the final season. We got 535 rows.

### Q6 — Performers who worked in several languages

We joined credits for films rated 7 or higher with a source for original languages. We kept performers with at least two films and at least two original languages. The source covers only part of IMDb, so we left out films with no language match. We got 5,826 rows. Of those, 2,391 reached three languages or more. To find them in "q6.csv", filter for "at_least_three_languages=true".

### Q7 — Performers who reached their highest rating early in their career

We kept performers with at least 15 films across at least 20 years. We used films that met the minimum vote count to find their highest rating. For tied ratings, we took the most recent film. It had to be in the first quarter of the performer's career. We got 6,158 rows.

### Q8 — Having several roles on the same film

We kept people with at least two types of roles across their career. For each person, we grouped films by whether they had one role or several. We counted films and averaged ratings in each group. We got 127,468 rows. For 87,757 people, every film in the analysis fell in the single-role group. The group sizes matter when comparing averages. If a person had several roles on just one film, that group's average is the rating of that film.

## Film enrichment

We kept up to ten films per genre and year. We ranked them by rating and used vote counts to break ties. A film can appear in several genres. The 9,089 ranking places represent 6,256 different films.

We used IMDb IDs to find the right pages through Wikidata. Then we collected plot summaries and biographies from Wikipedia. We saved the source URLs and cached responses so we could resume without repeating requests. Spark processed the texts and joined them to films and credits. We recorded a status for missing texts.

Out of 6,256 films, 5,174 have a plot summary and 5,369 have at least three biographies. Both are available for 4,710 films, about three quarters of the selection.

A second source could fill some gaps for the 1,546 incomplete films. We would check the IMDb ID when available. We could also check the title and year for a film, or the name and credits for a person. We would check the licence and keep the URL of the text we used.

## Wikipedia streaming

The producer sent Wikipedia edits to Kafka. Spark read and filtered them, then looked for the edited pages in our Wikipedia–IMDb catalogue. The live capture lasted about 15 minutes and 26 seconds. Spark read 1,845 events. Of those, 1,311 passed the filters. Seven matched the catalogue across six pages.

We replayed an earlier capture to check the processing. We also used the checkpoint to test recovery after an interruption. To test late events, we used the time of the edit. The synthetic test used one-minute windows and a two-minute watermark.

Two minutes was enough to show both cases with a few edits. One late edit reached an open window. Spark dropped one that reached a closed window. With a longer watermark delay, Spark could count more late edits. It would keep the window data longer and publish final counts later.

## Open question — Collaborations between directors and performers

We wanted to see if films got higher ratings at the start of a collaboration. For each director–performer pair, we compared film ratings from its first year in the data with ratings from later years. We used films with at least 1,000 votes and pairs that worked together in at least two different years.

Across 16,884 pairs, the average was 6.53 in the first year and 6.18 later, about 0.35 points lower. Some pairs may have worked together before their first year in our data. The kinds of films they made and their popularity could also help explain the gap.

We gave each pair the same weight to describe the change for a pair across the two periods. If we gave each film the same weight, a pair with twenty films could weigh more than a pair with two. That would describe the films as a whole.

## Running the project and finding the results

You can run the project with Docker on Linux or Windows. The commands are in "PROJECT.md". Start with synthetic data, then try a small IMDb sample before using the seven full files. IMDb updates its files, so row counts can change between downloads.

The validation tables and output previews are in "validation/". To reproduce the figures, use the verified Parquet files from run 43125611999 and its "VERIFIED.json" receipt. Generate nine CSV files locally, one for each question from Q1 to Q8 and one for the ranking. The manifest records the counts and checksums for that snapshot.
