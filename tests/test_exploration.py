from imdb_preparation.explore import analyses


def test_all_explorations_and_top10_on_controlled_data(spark):
    titles = []
    ratings = []
    principals = []
    crew = []
    episodes = []
    for i in range(16):
        key = f"tt{10000 + i:07d}"
        titles.append(
            (
                key,
                "movie",
                f"Film {i}",
                1980 + i * 3 if i < 15 else 2025,
                400 if i == 15 else 100,
                ["Drama", "Comedy"] if i == 0 else ["Drama"],
            )
        )
        rating = {0: 9.0, 1: 5.5, 2: 6.0, 3: 8.5, 4: 8.0}.get(i, 6.0)
        ratings.append((key, rating, 2000))
        if i < 15:
            principals.append((key, 1, "nm0000001", "actor"))
        crew.append(
            (key, ["nm0000002"] if i < 3 else ["nm0000001"] if i == 3 else None, None)
        )
    for season in range(1, 4):
        for number in range(1, 11):
            key = f"tt{20000 + season * 10 + number:07d}"
            episodes.append((key, "tt0099999", season, number))
            ratings.append(
                (
                    key,
                    9.8
                    if season == 1 and number == 1
                    else 2.0
                    if season == 3 and number == 10
                    else 7.0,
                    50,
                )
            )
    titles.append(("tt0099999", "tvSeries", "Series", 2000, None, None))
    for i in range(12):
        key = f"tt{30000 + i:07d}"
        titles.append((key, "movie", "Ranking tie", 2000, 100, ["Drama"]))
        ratings.append((key, 6.0, 2000))
    tables = {
        "title.basics": spark.createDataFrame(
            titles,
            "tconst string,titleType string,primaryTitle string,startYear long,runtimeMinutes long,genres array<string>",
        ),
        "title.ratings": spark.createDataFrame(
            ratings, "tconst string,averageRating double,numVotes long"
        ),
        "name.basics": spark.createDataFrame(
            [("nm0000001", "Actor"), ("nm0000002", "Director")],
            "nconst string,primaryName string",
        ),
        "title.principals": spark.createDataFrame(
            principals, "tconst string,ordering long,nconst string,category string"
        ),
        "title.crew": spark.createDataFrame(
            crew, "tconst string,directors array<string>,writers array<string>"
        ),
        "title.episode": spark.createDataFrame(
            episodes,
            "tconst string,parentTconst string,seasonNumber long,episodeNumber long",
        ),
    }
    config = dict(
        minimum_runtime=40,
        minimum_votes=1000,
        episode_minimum_votes=10,
        runtime_zscore=2,
        success_rating=7,
        early_career_fraction=0.25,
    )
    langs = spark.createDataFrame(
        [
            ("tt0010000", "en", "https://example.invalid/1"),
            ("tt0010003", "fr", "https://example.invalid/2"),
            ("tt0010004", "de", "https://example.invalid/3"),
        ],
        "tconst string,language string,source_url string",
    )
    outputs = analyses(tables, config, langs)
    assert outputs["q1"].filter("tconst='tt0010015'").count() == 1
    assert outputs["q2"].groupBy("startYear").count().filter("count > 3").count() == 0
    assert outputs["q3"].count() > 0
    assert {r.nconst for r in outputs["q4"].collect()} == {"nm0000002"}
    assert outputs["q5"].first().parentTconst == "tt0099999"
    assert outputs["q6"].first().at_least_three_languages
    assert outputs["q7"].first().nconst == "nm0000001"
    assert outputs["q8"].filter("multiple_roles").count() == 1
    assert (
        outputs["top10_per_genre_year"]
        .groupBy("startYear", "genre")
        .count()
        .filter("count > 10")
        .count()
        == 0
    )
    assert (
        outputs["top10_per_genre_year"]
        .filter("startYear=2000 AND genre='Drama'")
        .count()
        == 10
    )
    assert analyses(tables, config)["q6"] is None
