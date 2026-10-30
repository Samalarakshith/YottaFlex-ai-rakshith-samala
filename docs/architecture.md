# Architecture

```text
CSV (pandas chunks) / JSONL (one line at a time)
                 |
                 v
        schema + domain validation <---- customer reference CSV
                 |
         +-------+--------+
         |                |
         v                v
 SQLite target/events   run-specific quarantine CSV
         |                |
         v                v
 atomic processed CSV   reject reasons + raw source record
         |
         v
 JSON run report + SQLite run metadata
```

The target is an append-only event table keyed by `event_id`. Identical duplicate payloads are counted and ignored; a conflicting payload with an existing ID is quarantined. Source event time is preserved in UTC and is distinct from processing time.


## Contract-aware ingestion

`contracts/events_v1.json` is the versioned producer contract. Before ingestion, the pipeline compares the observed batch columns with required contract fields and reports breaking missing columns versus non-breaking additive columns. Record-level validation checks required/non-null fields, decimal and timestamp parsing, minimum amount, and allowed values. The run report embeds the compatibility result. `scripts/compare_contracts.py` compares two contract versions and exits with code 2 for breaking changes, making it suitable for CI checks.
