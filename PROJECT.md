# Big Data Processing — gr-04

This guide describes how to set up, run, and verify the ECE Big Data Processing project. The students present their methods, results, and interpretation in `REPORT.md`.

## Contributors

| Name | GitHub | Email |
| --- | --- | --- |
| Nicolas Rihana | nicohris | nrihana1@gmail.com |
| Benjamin Labos | benjaminlabos | benjamin.labos@edu.ece.fr |

## Project structure

| Component | Entry point |
| --- | --- |
| IMDb download, preparation, profiling, and analysis | `imdb_preparation/` and the commands below |
| Wikipedia event streaming with Kafka and Spark | [Streaming guide](PROJECT_STREAMING.md) |
| Synopsis and biography enrichment | [Enrichment guide](project/enrichment/README.md) |
| Director–performer collaboration analysis | [Collaboration analysis guide](project/collaborations/README.md) |

The pipeline separates batch preparation and analysis, live event processing, and enrichment. The batch stages write reusable Parquet tables, and the components exchange explicit datasets keyed by IMDb identifiers. This lets us check a join, replay a stream, or rerun an analysis independently. Manifests record inputs and output checks so we can trace results back to the data and settings used.

## Setup and smoke tests

Run commands from the repository root. Use fresh input and output directories for each run. The native Linux path was tested with Python 3.12.14, OpenJDK 21.0.13, and PySpark 4.2.0.

```bash
git clone https://github.com/benjaminlabos/ece-big-data-processing.git
cd ece-big-data-processing
git status --short --branch
python -m venv .venv
.venv/bin/python -m pip install -r requirements-preparation.txt
.venv/bin/python -m pytest -q tests
```

Start with the synthetic fixture, then run a small real IMDb sample.

```bash
.venv/bin/python -m imdb_preparation.fixture --destination data/raw/fixture-v1
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/fixture-v1 --output results/fixture-v1 --as-of-year 2026
```

```bash
.venv/bin/python -m imdb_preparation.download --destination data/raw/imdb-sample-2026-11-10 --rows 1000
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/imdb-sample-2026-11-10 --output results/sample-2026-11-10 --as-of-year 2026
```

The fixture exercises all seven tables, including missing and invalid values, duplicate keys, and invalid references. The sample downloads only the first 1,000 rows per table, so unresolved references in the sample are retained as unverified. Use the complete dataset for reference-quality statistics.

### Windows PowerShell with Docker

Install Docker Desktop with its Linux engine and Compose v2. Clone the repository using the Git command above and enter the clone. The preparation service uses the pinned Spark image and runs without Kafka or Jupyter.

```powershell
git status --short --branch
docker info
docker compose -f compose.preparation.yaml config -q
docker compose -f compose.preparation.yaml pull preparation
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.fixture --destination data/raw/fixture-win-v1
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.prepare --raw-dir data/raw/fixture-win-v1 --output results/fixture-win-v1 --as-of-year 2026
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.download --destination data/raw/imdb-sample-win-v1 --rows 1000
docker compose -f compose.preparation.yaml run --rm preparation python -m imdb_preparation.prepare --raw-dir data/raw/imdb-sample-win-v1 --output results/sample-win-v1 --as-of-year 2026
```

To run the preparation tests inside the container:

```powershell
docker compose -f compose.preparation.yaml run --rm preparation bash -c 'python -m pip install --user pytest==9.0.2 && python -m pytest -q tests'
```

## Full IMDb run

The complete download includes all seven IMDb datasets. Budget disk space for the compressed inputs, Spark working files, Parquet outputs, and quarantined records. The downloader records source metadata and SHA-256 hashes. IMDb updates its data daily, so a fresh download can produce different results.

```bash
.venv/bin/python -m imdb_preparation.download --destination data/raw/imdb-full-v1
.venv/bin/python -m imdb_preparation.prepare --raw-dir data/raw/imdb-full-v1 --output results/full-v1 --shuffle-partitions 32 --as-of-year 2026
```

Check for `SUCCESS.json` and inspect the manifests and quality outputs before using a run downstream. For the full analysis, the raw profile, the original-language reference, the analysis configuration, and the intermediate outputs must refer to the same run. Follow the [full-run commands and recorded checkpoints](https://github.com/benjaminlabos/ece-big-data-processing/blob/bd71ef21b95cc4338b34625fca656ef927c0cb22/PROJECT.md#exécution-imdb-complète-et-exploration--branche-featimdb-full-analysis) for profiling, exploration, selection, and enrichment inputs. The historical notes preserve the inputs, resource settings, and recovery procedures for the verified runs.

## Verified outputs and demonstration

- The complete IMDb preparation and eight Spark queries, plus the year–genre ranking, were checked in [run 43125611999](https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43125611999).
- The open-ended collaboration analysis was checked in [run 43128551838](https://github.com/benjaminlabos/ece-big-data-processing/actions/runs/43128551838).
- The enrichment v2 archive and the recorded streaming replay were audited. The replay contains 1,845 unique events and seven matches. To check the replay data offline, run `python validation/check_demo_payload.py`.
- See the [demonstration guide](validation/DEMONSTRATION-2026-11-13.md) for commands and evidence, the [integration checks](validation/INTEGRATION-CANDIDATE.md) for a clean-clone check, and the [release checklist](validation/FINAL-RELEASE-CHECKLIST.md) for the status of the submission.
- The [query export guide](deliverables/query-outputs/README.md) documents how to generate the nine complete CSVs locally and check them against their manifest.

## Use of AI tools

Nicolas used OpenAI Codex to help inspect the repository and official documentation, set up the environment, implement and debug PySpark and download code, write and run tests, and draft technical documentation and pull request descriptions. The assistance also supported learning: we used it to understand Spark behavior, explore implementation options, and work through failures.

The team discussed the architecture and reviewed its trade-offs, including how to separate batch processing from streaming, define the data interfaces, and make runs reproducible. We checked the implementation through peer review, execution, and comparisons with saved results. The reported figures come from executed code and checked artifacts. The students will write the explanations and reasoning in `REPORT.md` themselves. We will update this disclosure before submission to reflect any additional use by either contributor.

## References

- [Project instructions](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/project_instructions.md)
- [Submission instructions](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/README.md)
- [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/)

Information courtesy of IMDb (https://www.imdb.com). Used with permission.
