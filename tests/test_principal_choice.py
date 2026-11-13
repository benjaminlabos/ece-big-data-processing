from imdb_preparation.choose_principals import choose


def test_director_performer_overlap_and_reserves_are_distinct(spark):
    rows = [
        ("tt0000001", "nm0000001", 1, "actor"),
        ("tt0000001", "nm0000001", 9, "director"),
        ("tt0000001", "nm0000002", 10, "director"),
        ("tt0000001", "nm0000003", 2, "actress"),
        ("tt0000001", "nm0000004", 3, "actor"),
        ("tt0000001", "nm0000005", 4, "writer"),
        ("tt0000001", "nm0000006", 5, "producer"),
        ("tt0000002", "nm0000007", 1, "self"),
    ]
    credits = spark.createDataFrame(
        rows, "tconst string,nconst string,ordering long,category string"
    )
    primary, reserves = choose(credits)
    first = {r.nconst for r in primary.filter("tconst='tt0000001'").collect()}
    assert first == {"nm0000001", "nm0000003", "nm0000004"}
    backup = {r.nconst for r in reserves.filter("tconst='tt0000001'").collect()}
    assert backup == {"nm0000002", "nm0000005"}
    assert not (first & backup)
    assert primary.filter("tconst='tt0000002'").first().nconst == "nm0000007"
