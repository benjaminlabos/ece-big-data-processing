"""Spark expressions for profiling, auditable repairs and key validation."""

from functools import reduce

from pyspark.sql import Window, functions as F

from .schema import ARRAYS, INTEGERS, KEYS, TABLES

AUDIT_SCHEMA = "table string, key_json string, column string, rule string, original_value string, action string"
QUARANTINE_SCHEMA = "table string, key_json string, row_json string, reason string"


def missing(column):
    value = F.col(column)
    return value.isNull() | (F.trim(value) == "") | (F.trim(value) == "\\N")


def key_json(table):
    # Same key representation before and after numeric type enforcement.
    return F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in KEYS[table]]))


def row_json(table):
    return F.to_json(F.struct(*[F.col(c) for c in TABLES[table]]))


def profile(table, raw):
    """One aggregate, original row denominator (including duplicate/keyless rows)."""
    columns = TABLES[table]
    totals = (
        raw.agg(
            F.count(F.lit(1)).alias("_rows"),
            *[F.sum(F.when(missing(c), 1).otherwise(0)).alias(c) for c in columns],
        )
        .first()
        .asDict()
    )
    count = totals.pop("_rows")
    return count, [
        {
            "table": table,
            "column": c,
            "total_rows": count,
            "missing_rows": totals[c] or 0,
            "missing_percent": 100.0 * (totals[c] or 0) / count if count else None,
        }
        for c in columns
    ]


def normalize(raw):
    return raw.select(
        *[
            F.when(missing(c), F.lit(None).cast("string"))
            .otherwise(F.trim(F.col(c)))
            .alias(c)
            for c in raw.columns
        ]
    )


def quarantine(table, frame, reason):
    return frame.select(
        F.lit(table).alias("table"),
        key_json(table).alias("key_json"),
        row_json(table).alias("row_json"),
        F.lit(reason).alias("reason"),
    )


def valid_id(column, prefix):
    return F.col(column).rlike(r"^" + prefix + r"[0-9]{7,}$")


def partition_keys(table, frame):
    """Exact duplicates collapse; every conflicting-key row is quarantined."""
    distinct = frame.dropDuplicates()
    checks = []
    for c in KEYS[table]:
        if c == "ordering":
            checks.append(
                F.col(c).rlike(r"^[0-9]+$") & (F.expr(f"try_cast(`{c}` as bigint)") > 0)
            )
        else:
            checks.append(valid_id(c, "nm" if c == "nconst" else "tt"))
    valid = reduce(lambda a, b: a & b, checks)
    tagged = distinct.withColumn("_valid_key", F.coalesce(valid, F.lit(False)))
    invalid = quarantine(
        table, tagged.filter(~F.col("_valid_key")), "invalid_primary_key"
    )
    keyed = tagged.filter("_valid_key").drop("_valid_key")
    # Canonical ordering prevents "01" and "1" from evading uniqueness checks.
    if "ordering" in KEYS[table]:
        keyed = keyed.withColumn(
            "ordering", F.expr("try_cast(ordering as bigint)").cast("string")
        )
    keyed = keyed.withColumn(
        "_key_count", F.count(F.lit(1)).over(Window.partitionBy(*KEYS[table]))
    )
    conflicts = quarantine(
        table, keyed.filter("_key_count > 1"), "conflicting_primary_key"
    )
    good = keyed.filter("_key_count = 1").drop("_key_count")
    return distinct, good, invalid.unionByName(conflicts)


def audit_events(table, frame, rules):
    """Explode only matching rules; no collect of source records on driver."""
    if not rules:
        return frame.sparkSession.createDataFrame([], AUDIT_SCHEMA)
    entries = [
        F.when(
            F.coalesce(condition, F.lit(False)),
            F.struct(
                F.lit(column).alias("column"),
                F.lit(rule).alias("rule"),
                F.col(column).cast("string").alias("original_value"),
                F.lit(action).alias("action"),
            ),
        )
        for column, rule, condition, action in rules
    ]
    return frame.select(
        F.lit(table).alias("table"),
        key_json(table).alias("key_json"),
        F.explode(F.filter(F.array(*entries), lambda x: x.isNotNull())).alias("event"),
    ).select("table", "key_json", "event.*")


def clean_values(table, frame, as_of_year):
    rules = []
    conditions = {}
    types = {c: "bigint" for c in INTEGERS[table]}
    if table == "title.ratings":
        types["averageRating"] = "double"

    for c, kind in types.items():
        value = F.col(c)
        number = F.expr(f"try_cast(`{c}` as {kind})")
        lexical = value.rlike(
            r"^[0-9]+$" if kind == "bigint" else r"^[0-9]+(?:\.[0-9]+)?$"
        )
        malformed = value.isNotNull() & (~lexical | number.isNull())
        rules.append((c, "invalid_numeric", malformed, "set_null"))
        conditions[c] = malformed
        if c.endswith("Year"):
            invalid_range = (number < 1) | (number > 9999)
            if table == "name.basics":
                invalid_range = invalid_range | (number > as_of_year)
        elif c in ("isAdult", "isOriginalTitle"):
            invalid_range = ~number.isin(0, 1)
        elif c == "averageRating":
            invalid_range = (number < 1) | (number > 10) | F.isnan(number)
        elif c in ("runtimeMinutes", "ordering"):
            invalid_range = number <= 0
        else:
            invalid_range = number < 0
        invalid_range = value.isNotNull() & ~malformed & invalid_range
        rules.append((c, "out_of_range", invalid_range, "set_null"))
        conditions[c] = conditions[c] | invalid_range

    for c in frame.columns:
        if c in ("tconst", "titleId", "parentTconst", "nconst"):
            bad = F.col(c).isNotNull() & ~valid_id(c, "nm" if c == "nconst" else "tt")
            rules.append((c, "invalid_identifier", bad, "set_null"))
            conditions[c] = bad

    events = audit_events(table, frame, rules)
    typed = frame.select(
        *[
            (
                F.when(F.coalesce(conditions[c], F.lit(False)), F.lit(None))
                .otherwise(F.col(c))
                .cast(types.get(c, "string"))
                if c in conditions
                else F.col(c)
            ).alias(c)
            for c in frame.columns
        ]
    )
    pairs = []
    if table == "name.basics":
        pairs.append(
            (
                "deathYear",
                "death_before_birth",
                F.col("deathYear") < F.col("birthYear"),
                "set_null",
            )
        )
    if table == "title.basics":
        pairs.append(
            (
                "endYear",
                "end_before_start",
                F.col("endYear") < F.col("startYear"),
                "set_null",
            )
        )
    events = events.unionByName(audit_events(table, typed, pairs))
    for c, _, condition, _ in pairs:
        typed = typed.withColumn(
            c, F.when(condition, F.lit(None).cast("bigint")).otherwise(F.col(c))
        )

    array_rules = []
    for c, delimiter in ARRAYS.get(table, {}).items():
        tokens = F.transform(F.split(F.col(c), delimiter), lambda x: F.trim(x))
        good_tokens = F.filter(tokens, lambda x: (x != "") & (x != "\\N"))
        array_rules.extend(
            [
                (
                    c,
                    "missing_array_element",
                    F.size(tokens) != F.size(good_tokens),
                    "remove_missing_elements",
                ),
                (
                    c,
                    "duplicate_array_element",
                    F.size(good_tokens) != F.size(F.array_distinct(good_tokens)),
                    "deduplicate_elements",
                ),
            ]
        )
    events = events.unionByName(audit_events(table, typed, array_rules))
    for c, delimiter in ARRAYS.get(table, {}).items():
        tokens = F.transform(F.split(F.col(c), delimiter), lambda x: F.trim(x))
        values = F.array_distinct(F.filter(tokens, lambda x: (x != "") & (x != "\\N")))
        typed = typed.withColumn(
            c,
            F.when(F.size(values) > 0, values).otherwise(
                F.lit(None).cast("array<string>")
            ),
        )
    return typed, events


def check_reference(table, frame, column, parent, parent_key, is_array, repair):
    """Anti join against unique parent keys; samples only report absent refs."""
    keys = KEYS[table]
    values = (
        frame.select(*keys, F.explode(column).alias("_reference"))
        if is_array
        else frame.select(*keys, F.col(column).alias("_reference"))
        if column not in keys
        else frame.select(*keys).withColumn("_reference", F.col(column))
    )
    values = values.filter(F.col("_reference").isNotNull())
    absent = values.join(
        parent.select(F.col(parent_key).alias("_reference")).distinct(),
        "_reference",
        "left_anti",
    )
    action = (
        "remove_reference"
        if is_array
        else ("quarantine_row" if column in keys else "set_null")
    )
    events = absent.select(
        F.lit(table).alias("table"),
        key_json(table).alias("key_json"),
        F.lit(column).alias("column"),
        F.lit(
            "reference_not_in_clean_parent" if repair else "reference_not_in_snapshot"
        ).alias("rule"),
        F.col("_reference").alias("original_value"),
        F.lit(action if repair else "retained_unverified").alias("action"),
    )
    empty = frame.sparkSession.createDataFrame([], QUARANTINE_SCHEMA)
    if not repair:
        return frame, events, empty
    bad = absent.groupBy(*keys).agg(F.collect_set("_reference").alias("_bad_refs"))
    joined = frame.join(bad, keys, "left")
    if column in keys:
        rejected = quarantine(
            table,
            joined.filter(F.col("_bad_refs").isNotNull()),
            f"orphan_primary_key:{column}",
        )
        return (
            joined.filter(F.col("_bad_refs").isNull())
            .drop("_bad_refs")
            .select(*frame.columns),
            events,
            rejected,
        )
    if is_array:
        remaining = F.filter(
            F.col(column), lambda x: ~F.array_contains(F.col("_bad_refs"), x)
        )
        replacement = F.when(F.size(remaining) > 0, remaining).otherwise(
            F.lit(None).cast("array<string>")
        )
    else:
        replacement = F.lit(None).cast(frame.schema[column].dataType)
    result = joined.withColumn(
        column,
        F.when(F.col("_bad_refs").isNotNull(), replacement).otherwise(F.col(column)),
    )
    return result.drop("_bad_refs").select(*frame.columns), events, empty
