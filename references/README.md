# Original-language reference snapshot

`wikidata-p364-2026-11-11.tar.gz` contains the original CSV, its original
manifest and the ISO vocabulary captured from https://query.wikidata.org/sparql
on 2026-11-11 at 13:32:34 UTC. It contains 369,502 CSV records. This is an
observed Wikidata reference, not an exhaustive IMDb language inventory.

License: CC0 1.0 — https://creativecommons.org/publicdomain/zero/1.0/
Only structured identifiers, language statements and source URIs are included;
no generated synopsis or biography text is included.

CSV SHA256: `51f9809c7f47431846a266bb08f0c8177d15961367d04d6b27069aa8d8784185`
Vocabulary SHA256: `842f4bc65bf3e7e2174626412cd0d5960cb9e96a20998f0836d8a8ab69ddffa8`
Archive SHA256: `d5bbf5c61842eb78a4c7dd12a61fa58237c4823f61183118e0846dae41ce25ca`
Archive size: 3,170,216 bytes.

Restore without contacting Wikidata, preserving original bytes/provenance:

```bash
python -m imdb_preparation.languages --restore-archive references/wikidata-p364-2026-11-11.tar.gz --output data/reference/restored-original-languages
```

Spark performs validation of language entities, ISO mapping, deduplication,
coverage measurement and joins in `imdb_preparation.explore`. This historical
snapshot is not relabelled as a newly paginated collection.
