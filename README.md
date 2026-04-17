# aksjeeierbok-state-materializer

Nightly Cloud Run Job that materializes year-end shareholder-state
snapshots from the aksjeeierbok changelog.

## Why

`aksjeeierbok/cdc/changelog/{yr}.parquet` is an *event log* — rows
reflect events that happened IN year {yr}. A shareholder who stayed
put from 2018–2023 produces one `new` row in 2018 and nothing
afterwards.

Downstream consumers (lineage-weights, cap-table queries, ownership
history) need **cap-table state as of year-end Y**: "who held what
in this company on Dec 31 Y?". Reconstructing that per-query by
scanning 21 years of changelog is wasteful.

This job pre-computes the state once per day and writes:

```
gs://sondre_brreg_data/aksjeeierbok/state/by_year/{yr}.parquet
```

## Output schema

One row per `(orgnr, shareholder_id, aksjeklasse)` that holds non-zero
ownership at year-end yr. Key = document_id from the original CDC
changelog.

| column | type | source |
|---|---|---|
| orgnr | string | changelog |
| document_id | string | changelog (= "orgnr\|shareholder_id\|aksjeklasse") |
| shareholder_id | string | details_json |
| shareholder_type | string | details_json (company/person/foreign) |
| aksjeklasse | string | details_json |
| landkode | string | details_json |
| margin | string | details_json (extensive/compact) |
| antall_aksjer | double | details_json.curr_antall_aksjer |
| ownership_pct | double | details_json.curr_ownership_pct |
| as_of_year | int | output year |
| valid_time | date | year-end of as_of_year (= as_of_year + "-12-31") |
| last_event_type | string | changelog (last event that wasn't 'disappeared') |

## Algorithm

For each year Y in 2004..current:
1. Read all changelog rows with `valid_time <= Y-12-31` (= all files
   for years <= Y)
2. Window function: keep the latest row per `document_id`
3. Drop rows whose latest event is `disappeared`
4. Parse fields from `details_json`
5. Write Y's snapshot

DuckDB runs all of this in-process. ~15M rows total across 21 years,
so full rebuild is <5 min on 4 CPU.

## Schedule

Nightly 08:30 Oslo — after aksjeeierbok-cdc finishes writing today's
changelog. Current-year snapshot rebuilds every day; prior years are
immutable but the cost of re-materializing them is small so we rebuild
everything for idempotency.

## Methodology version

`2.0.0` — supersedes the implicit v1 inside lineage-weights that
treated the changelog as a state table. See
`lineage-weights/LIMITATIONS.md` for context.
