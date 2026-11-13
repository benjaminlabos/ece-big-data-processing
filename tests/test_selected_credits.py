from imdb_preparation.export_selected_credits import select_credit_rows
from imdb_preparation.quality import normalize, partition_keys


def test_filter_before_normalization_preserves_selected_key_conflicts(spark):
    raw = spark.createDataFrame(
        [
            ("tt0000001", "01", "nm0000001", "actor", "\\N", "[]"),
            ("\u00a0tt0000001\u00a0", "1", "nm0000001", "actor", "\\N", "[]"),
            ("tt0000001", "2", "nm0000002", "actor", "\\N", "[]"),
            ("tt0000001", "2", "nm0000003", "actor", "\\N", "[]"),
            ("tt0000002", "1", "nm0000001", "actor", "\\N", "[]"),
        ],
        "tconst string,ordering string,nconst string,category string,job string,characters string",
    )
    chosen = spark.createDataFrame([("tt0000001",)], "tconst string")
    _, baseline, _ = partition_keys("title.principals", normalize(raw))
    baseline = baseline.join(chosen, "tconst", "left_semi")
    _, optimized, rejected = partition_keys(
        "title.principals", normalize(select_credit_rows(raw, chosen))
    )
    assert (
        baseline.exceptAll(optimized).count()
        == optimized.exceptAll(baseline).count()
        == 0
    )
    assert optimized.count() == 1
    assert rejected.count() == 2
