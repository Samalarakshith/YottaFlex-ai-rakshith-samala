# Data dictionary

## Source event (`orders.csv` / `orders.jsonl`)

| Field | Type | Required | Rule |
|---|---|---:|---|
| `event_id` | string | yes | stable event primary key |
| `order_id` | string | yes | order identifier; multiple events may refer to one order |
| `customer_id` | string | yes | must exist in customer reference file |
| `event_type` | string | yes | created, paid, shipped, cancelled |
| `amount` | decimal | yes | finite, non-negative; normalized to two decimals |
| `currency` | string | yes | USD in this demonstration |
| `event_timestamp` | ISO-8601 string | yes | normalized to UTC; naive input is assumed UTC |

## Target (`events.csv` and SQLite `events`)
Adds `event_date` derived from UTC timestamp, `source_file`, `source_row`, and `first_run_id`. `event_id` is unique. SQLite stores amount as a normalized decimal string to avoid binary floating-point rounding. The source file and row are retained for traceability.

## Metadata
`runs` stores run ID, source file, status, start/end timestamps, input/inserted/duplicate/rejected counts, and failure reason. `rejected` CSVs store run ID, source file, source row, reason, and serialized raw record.

## Grain and relationships
One target row per unique event ID. Many events may belong to one order; many events may refer to one customer. The customer reference CSV is treated as a set of known customer IDs. This is a synthetic event dataset, not a full order-state model.


## Versioned contract

`contracts/events_v1.json` is the executable contract of record. It repeats the required field definitions with explicit `type`, `required`, and `nullable` attributes; `event_type` and `currency` include allowed values, and `amount` includes a minimum of zero. The `primary_key` is `event_id`; the target grain is one row per unique event. Contract version comparisons are performed with `scripts/compare_contracts.py`.
