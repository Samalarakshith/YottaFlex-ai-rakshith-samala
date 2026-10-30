# StreamETL — memory-efficient CSV/JSONL pipeline

A reproducible local data-engineering exercise that reads large CSV files in bounded Pandas chunks or JSON Lines one record at a time, validates and transforms records, quarantines invalid data, stores events and run metadata in SQLite, and publishes a clean CSV plus JSON report. Uses synthetic data only.

## Requirements

- Python 3.10+ (tested target: Windows PowerShell; newer Python versions may need compatible Pandas wheels).
- Dependencies in `requirements.txt`.

## Setup (Windows PowerShell)

```powershell
cd streametl
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

If PowerShell blocks activation, use `Set-ExecutionPolicy -Scope Process Bypass` in that terminal or invoke `.venv\Scripts\python.exe` directly.

## Generate reproducible data

```powershell
python scripts/generate_data.py --out-dir data/sample --records 1000 --seed 20261030
python scripts/generate_data.py --out-dir data/large --records 100000 --seed 20261030
```

Generator creates `customers.csv`, `orders.csv`, `orders.jsonl`, and `expected.json`. Expected labels are separate from input data. Some deterministic data-quality issues are injected into the first rows when at least eight records are generated.

## Run the pipeline

```powershell
python -m streametl.cli --input data/sample/orders.csv --customers data/sample/customers.csv --db output/streametl.sqlite3 --output-dir output --chunk-size 500
python -m streametl.cli --input data/sample/orders.jsonl --customers data/sample/customers.csv --db output/jsonl.sqlite3 --output-dir output_jsonl --format jsonl --chunk-size 500
```

Outputs: `output/processed/events.csv`, run-specific files under `output/rejected/`, and run reports under `output/reports/`. SQLite stores the target and run metadata. Keep the same DB path when retrying if you want idempotency across runs.

## Data contracts and schema evolution

The active producer contract is `contracts/events_v1.json`. It declares the contract version, primary key/grain, required fields, primitive types, nullability, allowed values, and numeric minimums. The ingestion pipeline checks producer column compatibility before processing, validates each record against the contract and business rules, and includes a `contract_validation` section in every run report. CSV batches missing required fields fail as a breaking schema change; additive unknown columns are reported as non-breaking and ignored. JSONL records are validated individually because lines can have different shapes; malformed or incomplete records are quarantined with source row and raw payload.

Compatibility comparison is backward-compatibility oriented: removing a field, changing its type, relaxing non-nullability, making a previously required field optional, or removing allowed values is breaking. Adding an optional/nullable field is non-breaking. A new required field without a default is breaking. Adding an enum value is classified as non-breaking by this sample policy, but consumers that use exhaustive switches may need a stricter policy.

Compare contract versions:

```powershell
python scripts/compare_contracts.py --old contracts/events_v1.json --new contracts/events_v2_optional_region.json --report output/contract_compatible.json
python scripts/compare_contracts.py --old contracts/events_v1.json --new contracts/events_v2_breaking.json --report output/contract_breaking.json
```

Run ingestion with the default contract or supply another contract file:

```powershell
python -m streametl.cli --input data/sample/orders.csv --customers data/sample/customers.csv --db output/contracts.sqlite3 --output-dir output/contracts --contract contracts/events_v1.json
```

The checker compares contract definitions separately from inspecting producer batches. Header-based batch inspection cannot infer a column's declared logical type from CSV headers alone; actual values are therefore checked record by record.

## Tests

```powershell
pytest -q
```

Tests cover happy path, empty input, malformed JSONL, missing fields, nulls, duplicate and conflicting keys, invalid references, negative amounts, invalid event type/timestamp, unexpected schema, partial failure and retry, and late/out-of-order events.

## Benchmark

```powershell
python scripts/benchmark.py --records 100000 --chunk-size 5000 --work-dir data/benchmark
```

Benchmark output includes elapsed time, return code, and a JSON report. For authoritative peak pipeline RSS, measure the actual child process using Windows Performance Monitor/Task Manager or an OS process monitor; Python `tracemalloc` alone misses native allocations. Record only results actually measured on your machine. Compare 10k, 100k, and 1m records where time permits.

## Example execution evidence (measured)

A 100,000-row synthetic CSV run was executed during project preparation with chunk size 5,000. The recorded benchmark is in `examples/benchmark_report.json`; the concise summary is in `examples/benchmark_summary.json`. It completed successfully with 99,993 inserted events and 7 quarantined rows, with source accounting passing and the target amount total reconciling to the inserted amount total. The measured runtime and peak RSS are environment-specific; rerun the benchmark locally before citing them in a presentation.

The `examples/results/` directory contains sample CSV and JSONL run reports, published outputs, and quarantine files. These are execution artifacts from the included deterministic sample, not hand-written expected results.

## Reconciliation and queries

The JSON report has input, inserted, duplicate, rejected, target totals, and accounting checks. The accounting identity is `input_records = inserted_records + duplicate_records + rejected_records` for records/lines successfully yielded by the reader. A file-level parse/schema failure creates a failed run report instead.

Inspect SQLite with Python or a SQLite browser. Example SQL:

```sql
SELECT status, COUNT(*) FROM runs GROUP BY status;
SELECT event_date, COUNT(*) AS events, SUM(CAST(amount AS REAL)) AS total_amount
FROM events GROUP BY event_date ORDER BY event_date;
SELECT COUNT(*) AS total_events, COUNT(DISTINCT event_id) AS unique_event_ids FROM events;
```

## Assumptions and limitations

- Input event schema is documented in `docs/data_dictionary.md`. CSV requires all seven required columns; extra columns warn and are ignored. JSONL must contain one JSON object per line; malformed lines are quarantined. Arbitrary nested JSON and a single giant JSON array are not supported.
- Only USD is accepted; amounts must be finite and non-negative. Naive timestamps are interpreted as UTC.
- Target grain is one row per `event_id`; same-ID same-payload repeats are ignored, conflicting payloads are quarantined. Events are append-only; there is no order-state aggregation.
- Customer reference data is loaded into memory as a set. If that reference dataset itself becomes huge, store it in SQLite and validate with indexed lookups.
- CSV is the portable published target; SQLite is the durable local target and metadata store. Atomic rename assumes source and destination are on the same filesystem.
- No credentials or external services are required.

## Architecture and trade-offs

See `docs/architecture.md` and `docs/design_decisions.md`. This is a focused vertical slice rather than a distributed platform.
