"""ETL orchestration, quarantine, idempotent target and atomic CSV publishing."""
import csv, json, os, time, uuid
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from .reader import read_csv_chunks, read_jsonl
from .validator import validate_record, payload_hash
from .metadata import connect
from .config import REQUIRED_COLUMNS, OUTPUT_COLUMNS
from .contracts import load_contract, inspect_batch_schema, validate_batch_schema, validate_record_contract


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_customers(path: str | Path) -> set[str]:
    customers = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "customer_id" not in [x.strip().lower() for x in reader.fieldnames]:
            raise ValueError("customer reference CSV must contain customer_id")
        key = next(x for x in reader.fieldnames if x.strip().lower() == "customer_id")
        for row in reader:
            value = (row.get(key) or "").strip()
            if value:
                customers.add(value)
    return customers


def _quarantine_writer(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    f = open(path, "w", newline="", encoding="utf-8")
    fields = ["run_id", "source_file", "source_row", "reject_reason", "raw_record"]
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    return f, writer


def _publish(conn, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp = output_path.with_suffix(output_path.suffix + ".tmp")
    query = "SELECT event_id,order_id,customer_id,event_type,amount,currency,event_timestamp,event_date,source_file,source_row,first_run_id FROM events ORDER BY event_timestamp,event_id"
    with open(temp, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)
        for row in conn.execute(query):
            writer.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]])
    os.replace(temp, output_path)


def run_pipeline(input_path: str | Path, customers_path: str | Path, db_path: str | Path,
                 output_dir: str | Path, input_format: str = "auto", chunk_size: int = 5000,
                 fail_after: int | None = None, contract_path: str | Path | None = None) -> dict:
    started = time.perf_counter()
    input_path, output_dir = Path(input_path), Path(output_dir)
    if chunk_size < 1: raise ValueError("chunk_size must be >= 1")
    if not input_path.exists(): raise FileNotFoundError(input_path)
    fmt = input_format.lower()
    if fmt == "auto": fmt = "jsonl" if input_path.suffix.lower() in {".jsonl", ".ndjson"} else "csv"
    if fmt not in {"csv", "jsonl"}: raise ValueError("format must be csv or jsonl")
    customer_ids = load_customers(customers_path)
    if contract_path is None:
        contract_path = Path(__file__).resolve().parents[2] / "contracts" / "events_v1.json"
    contract = load_contract(contract_path)
    schema_observed = inspect_batch_schema(input_path, fmt)
    contract_report = validate_batch_schema(schema_observed, contract)
    run_id = "run_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    output_dir.mkdir(parents=True, exist_ok=True)
    rejects_path = output_dir / "rejected" / f"{run_id}_rejects.csv"
    report_path = output_dir / "reports" / f"{run_id}_report.json"
    rejects_path.parent.mkdir(parents=True, exist_ok=True); report_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    counters = {"input_records": 0, "inserted_records": 0, "duplicate_records": 0, "rejected_records": 0}
    inserted_amount_total = Decimal("0.00")
    started_at = utcnow()
    conn.execute("INSERT INTO runs(run_id,source_file,status,started_at) VALUES(?,?,?,?)", (run_id, str(input_path.resolve()), "running", started_at)); conn.commit()
    qf, qw = _quarantine_writer(rejects_path)
    try:
        if not contract_report["compatible"] and fmt == "csv":
            raise RuntimeError("unexpected schema: breaking producer schema change: " + "; ".join(c["reason"] for c in contract_report["changes"] if c["severity"] == "breaking"))
        if fmt == "csv":
            # Validate headers before pandas starts; unexpected columns are reported and allowed.
            headers = list(pd.read_csv(input_path, nrows=0).columns)
            lower_headers = [str(h).strip().lower() for h in headers]
            missing = [c for c in REQUIRED_COLUMNS if c not in lower_headers]
            if missing: raise ValueError("unexpected schema: missing required column(s): " + ", ".join(missing))
            extras = sorted(set(lower_headers) - set(REQUIRED_COLUMNS))
            if extras: print("WARNING: unexpected columns accepted and ignored: " + ", ".join(extras))
            records = read_csv_chunks(input_path, chunk_size)
        else:
            records = read_jsonl(input_path)
        for source_row, raw in records:
            if fail_after is not None and counters["input_records"] >= fail_after:
                raise RuntimeError(f"simulated failure after {fail_after} input records")
            counters["input_records"] += 1
            try:
                if isinstance(raw, Exception): raise ValueError(f"malformed JSONL: {raw}")
                # Normalize header names; schema drift is warned/ignored for CSV and JSONL.
                normalized_raw = {str(k).strip().lower(): v for k, v in raw.items()}
                extra = set(normalized_raw) - set(REQUIRED_COLUMNS)
                if extra and fmt == "jsonl":
                    # Additive schema drift is accepted but visible in report/log.
                    pass
                clean = validate_record(normalized_raw, customer_ids)
                validate_record_contract(normalized_raw, contract)
                digest = payload_hash(clean)
                prior = conn.execute("SELECT payload_hash FROM events WHERE event_id=?", (clean["event_id"],)).fetchone()
                if prior:
                    if prior["payload_hash"] == digest:
                        counters["duplicate_records"] += 1
                    else:
                        raise ValueError("conflicting duplicate event_id: existing payload differs")
                else:
                    conn.execute("INSERT INTO events(event_id,payload_hash,order_id,customer_id,event_type,amount,currency,event_timestamp,event_date,source_file,source_row,first_run_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (clean["event_id"], digest, clean["order_id"], clean["customer_id"], clean["event_type"], clean["amount"], clean["currency"], clean["event_timestamp"], clean["event_date"], input_path.name, source_row, run_id))
                    counters["inserted_records"] += 1
                    inserted_amount_total += Decimal(clean["amount"])
                # Commit periodically so a crash can resume safely via deterministic dedupe.
                if counters["input_records"] % chunk_size == 0: conn.commit()
            except (ValueError, TypeError, KeyError) as exc:
                counters["rejected_records"] += 1
                qw.writerow({"run_id": run_id, "source_file": str(input_path), "source_row": source_row,
                             "reject_reason": str(exc), "raw_record": json.dumps(raw if not isinstance(raw, Exception) else {"raw_error": str(raw)}, ensure_ascii=False, default=str)})
        conn.commit()
        _publish(conn, output_dir / "processed" / "events.csv")
        status, failure = "success", None
    except Exception as exc:
        conn.commit()  # partial inserts are safe: retry recognizes identical event IDs
        status, failure = "failed", f"{type(exc).__name__}: {exc}"
    finally:
        qf.close()
    ended_at = utcnow()
    elapsed = round(time.perf_counter() - started, 4)
    target_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.execute("UPDATE runs SET status=?,ended_at=?,input_records=?,inserted_records=?,duplicate_records=?,rejected_records=?,failure_reason=? WHERE run_id=?",
        (status, ended_at, counters["input_records"], counters["inserted_records"], counters["duplicate_records"], counters["rejected_records"], failure, run_id)); conn.commit()
    target_amount_total = sum((Decimal(row[0]) for row in conn.execute("SELECT amount FROM events")), Decimal("0.00"))
    report = {"run_id": run_id, "status": status, "source_file": str(input_path), "format": fmt,
              "started_at": started_at, "ended_at": ended_at, "duration_seconds": elapsed,
              **counters, "contract_validation": contract_report,
              "inserted_amount_total": format(inserted_amount_total, ".2f"),
              "target_amount_total": format(target_amount_total, ".2f"), "target_total_records": target_count,
              "accounting_check": counters["input_records"] == counters["inserted_records"] + counters["duplicate_records"] + counters["rejected_records"],
              "reconciliation_passed": status == "success" and target_count == conn.execute("SELECT COUNT(DISTINCT event_id) FROM events").fetchone()[0],
              "reject_file": str(rejects_path), "target_file": str(output_dir / "processed" / "events.csv"),
              "failure_reason": failure, "notes": ["Naive timestamps are interpreted as UTC.", "Only USD is accepted in this sample.", "Target is append-only by event_id; conflicting payloads are quarantined."]}
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    conn.close()
    if status != "success":
        raise RuntimeError(f"Pipeline run {run_id} failed: {failure}. See {report_path}")
    return report
