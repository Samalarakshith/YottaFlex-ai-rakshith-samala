# Design decisions and trade-offs

- **Pandas chunks for CSV:** readable and bounded by chunk size. JSONL uses a line iterator; an arbitrary JSON array is deliberately not supported.
- **SQLite target:** local, transactional, easy to inspect, and sufficient for this exercise. For large concurrent production workloads, consider a server database or partitioned columnar lake.
- **Append-only event identity:** same ID + same canonical payload is an idempotent duplicate; same ID + different payload is quarantined as a conflict. No silent overwrite.
- **Recovery:** rows are committed incrementally. If a run fails after some commits, the run is marked failed and output publication is skipped. Reprocessing the same file safely identifies previously inserted rows as duplicates and continues with remaining rows.
- **Publication:** a complete target export is written to a temporary file and replaced atomically after successful input processing. Existing output remains intact on a failed run.
- **Schema drift:** missing required CSV columns fail the run. Additional CSV columns produce a warning and are ignored. JSONL records may contain extra keys; required fields are still validated.
- **Quarantine boundary:** record-level data-quality errors are quarantined. CSV structural/parser errors and missing required file-level columns fail the run. JSONL malformed lines are quarantined individually.
- **Currency:** USD only, intentionally explicit rather than silently mixing currencies.
- **Memory measurement:** benchmark script records wall time and attempts RSS sampling; report the measurement method and limitations. Memory varies by Python, pandas, OS, chunk size, and input width.
- **Optimization:** tune chunk size, avoid unnecessary DataFrame copies, and consider Parquet for analytical scans. The first implementation favors transparency and portability.


## Contract compatibility policy

The sample enforces backward compatibility for schema evolution. Removing a previously defined field, changing its logical type, relaxing non-nullability, relaxing requiredness, or removing previously allowed enum values is breaking. An optional/nullable additive field is non-breaking; a new required field without a default is breaking. Added enum values are treated as non-breaking here, with a documented caveat for exhaustive consumers. CSV schema failures stop the batch before record ingestion; JSONL shapes are heterogeneous, so malformed/incomplete records are traceably quarantined individually while batch-level schema observations remain visible in the report.
