# ECE Big Data Processing — Group gr-04

Nicolas Rihana (`nicohris`) and Benjamin Labos (`benjaminlabos`)

## Project overview

We started with the seven IMDb files. The first step was to clean them with PySpark so we could use the same data for the eight exploration questions, the film enrichment, and our own question about collaborations between directors and performers. We also worked on a streaming pipeline with Kafka and Spark Structured Streaming. The idea was to follow edits on Wikipedia and see which ones were related to titles in IMDb.

We kept the batch processing, enrichment, and streaming separate. If we change a calculation in one of the questions, we can start from the Parquet files we already prepared. For the enrichment, we can resume using the responses saved in the cache. For streaming, we can replay an earlier capture. We also use manifests to keep track of the inputs, parameters, and output files for each run.

## IMDb data preparation

We processed 216,502,833 rows across the seven tables and checked for missing values in 39 columns. The column with the most missing values was `title.akas.attributes`, at 99.47%. The [full profile](validation/full-snapshot-43093003953/missing-profile.csv) gives the percentage for each column. The [top five](validation/full-snapshot-43093003953/top5-missing.csv) are listed below.

| Column | Missing values |
| --- | ---: |
| `title.akas.attributes` | 99.47% |
| `title.basics.endYear` | 98.73% |
| `name.basics.deathYear` | 98.32% |
| `name.basics.birthYear` | 95.66% |
| `title.principals.job` | 80.66% |

One thing we found was that 28 people had a death year earlier than their birth year. We set `deathYear` to `null` and kept the rest of the record. Since we join on `nconst`, we can still find their credits. Using `0` could cause problems later if a calculation treated it as an actual year.

We also found two ratings rows with a title ID that was missing from the cleaned catalogue. We could not link these ratings to a title, so we put the rows in quarantine and recorded the reason. In total, 105 rows were quarantined. We counted field corrections like the 28 death years separately, since those rows stayed in the dataset.

## IMDb exploration

### Q1 — Films with unusual runtimes

We calculated the average runtime and standard deviation for each genre. Then we kept feature films whose runtime was at least two standard deviations away from the average. The threshold depends on the genre. For example, if the standard deviation is 10 minutes, a difference of 20 minutes is enough. If it is 25 minutes, the difference needs to be at least 50 minutes. We got 13,640 rows.

### Q2 and Q3 — Most popular genres

For each year from 1980 to 2025, we added up the votes for films in each genre and kept the top three. With this method, a genre can rank high because it has a lot of films, even if each film gets fewer votes. In Q3, we compared the top three with the other genres using total votes, the number of film–genre pairs, and the average rating. A film with several genres can appear in both groups. We got 138 rows for Q2 and 92 for Q3.

For example, in 1980 the top three genres had 7,551,475 votes across 244 film-genre pairs, with an average rating of 6.22. The other 19 genres had 9,436,537 votes across 441 pairs, with an average rating of 6.30. The [Q3 results by year](validation/overnight-43093003953/query-review/q3-preview.csv) contain all 92 rows.

### Q4 — Directors with one highly rated film

We looked for directors with exactly one film rated 8 or higher and at least two other films with an average rating of 6 or lower. One of those other films can still be above 6 as long as their average stays at 6 or lower. We got 47 rows.

### Q5 — Best episode near the start and worst episode at the end

We kept series and miniseries with at least three seasons of ten episodes or more. Then we looked at episodes with at least ten votes. The highest-rated episode had to be in one of the first two seasons, and the lowest-rated episode had to be in the final season. We counted all episodes when checking the size of each season. The ten-vote threshold only applies when we compare ratings. We got 535 rows.

### Q6 — Performers who worked in several languages

We started with films rated 7 or higher and joined their credits with a source that gives the original language of each film. We kept performers linked to at least two different films and at least two original languages. We also added a flag for performers who reach three languages or more. The source only covers part of IMDb, so films without a language match are left out of the calculation. The full output has 5,826 rows. Of those, 2,391 reached at least three languages. To get the full list, follow the [export guide](deliverables/query-outputs/README.md) and filter `q6.csv` for `at_least_three_languages=true`.

### Q7 — Performers who reached their highest rating early in their career

We kept performers with at least 15 films and credits spanning at least 20 years. Then we looked for their highest film rating among the films that met the minimum number of votes. If several films had the same highest rating, we used the most recent one. It had to fall within the first quarter of the performer's career. This means that if a performer reached the same rating again later in their career, we left them out. We got 6,158 rows.

### Q8 — Having several roles on the same film

We kept people with at least two types of roles across their career. For each film in the analysis, we checked whether they had one role or several. We then calculated the average rating and film count separately for films where they had one role and films where they had several. The output has 127,468 rows. For 87,757 people, the films in the analysis only had single-role entries. When comparing the averages for one person, we also need to look at how many films are in each group. If someone had several roles on just one film, the average for that group is simply the rating of that film.

## Film enrichment

We selected up to ten films per genre and year. We ranked them by rating and used the number of votes to break ties. A film can appear under several genres, so the 9,089 places in the ranking correspond to 6,256 different films.

To find the right pages, we started with the IMDb IDs, looked them up in Wikidata, and then collected plot summaries and biographies from Wikipedia. We kept the sources and cached the responses so we could resume the collection without making the same requests again. Spark processed the texts and joined them to the films and credits. When a text was missing, we recorded a status so we could find those cases in the results.

Out of the 6,256 films, 5,174 have a plot summary and 5,369 have at least three biographies. A total of 4,710 films meet both conditions, which is about three quarters of the selection.

We still had 1,546 films with something missing. A second source could give us some of the plot summaries or biographies we did not find. We would check the IMDb ID when it is available, or check details like the title and year for a film and the name and credits for a person. We would also check the licence and keep the URL of the text we used. [Enrichment results](project/enrichment/evidence/final-enrichment-v2-2026-11-13.json)

## Wikipedia streaming

The producer collects Wikipedia edits and sends them to Kafka. Spark reads the events, filters them, and checks whether the edited page is in our Wikipedia–IMDb catalogue. We ran a live capture for about 15 minutes and 26 seconds. Spark read back 1,845 events from that capture. After filtering, 1,311 edits met our criteria. Seven of those matched the catalogue, across six different pages.

We also replayed events from an earlier capture to check the processing. For late events, we tested the watermark, which uses the time when the edit happened. In the separate synthetic test, we used one-minute windows and a two-minute watermark. Spark dropped one late row. We used the checkpoint to test whether Spark could resume after an interruption. The streaming guide contains the commands and results for these tests and for the live capture.

We used two minutes for the small synthetic test because we could show both cases with just a few edits: one late edit still fits in an open window and one comes after its window is closed. This value was set for the test. With more time, Spark could keep more late edits, and it would keep the window data longer and give the final counts later. [Streaming test](PROJECT_STREAMING.md#horaires-et-test-des-%C3%A9v%C3%A9nements-tardifs)

## Open question — Collaborations between directors and performers

We wanted to see if films were rated higher at the start of a collaboration between a director and a performer. For each pair, we compared the ratings of their films in the first year we saw them work together in the data with the ratings of their films in later years. In the main analysis, we kept films with at least 1,000 votes and pairs that worked together in at least two different years. Each pair had the same weight in the final average.

For the 16,884 pairs we kept, the average was 6.53 in the first year and 6.18 in the following years. That is a difference of about 0.35 points. This is what we found with the data and thresholds we used. Some pairs may have worked together before the first year we can see in IMDb. The types of films they made and their popularity could also explain part of the difference.

We gave the same weight to each pair because we wanted to know how the rating changes for a pair between the two periods. If each film had the same weight, a pair with twenty films could count more than a pair with only two. The result would tell us more about the films as a whole. [Comparison results](validation/collaborations-full-43093003953/comparisons.csv)

## Running the project and finding the results

The `README.md` at the root of the repository points to `PROJECT.md`. The guide explains how to run the project with Docker on Linux or Windows. You can start with synthetic data, then try a small IMDb sample before running the full pipeline on the seven official files. IMDb updates these files, so the row counts can change if you download them on a different date.

The validation tables and output previews are in `validation/`. To reproduce the figures in this report, the export guide explains how to download the verified Parquet files from run `43125611999`, find `VERIFIED.json`, and generate the nine CSV files locally. There is one CSV for each question from Q1 to Q8 and one for the ranking. The row counts and checksums in the manifest refer to that snapshot.
