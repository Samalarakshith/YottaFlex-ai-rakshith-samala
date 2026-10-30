import csv, json
from pathlib import Path
import pytest
from streametl.pipeline import run_pipeline

FIELDS = ["event_id","order_id","customer_id","event_type","amount","currency","event_timestamp"]

def setup_files(tmp_path, rows):
    customers=tmp_path/"customers.csv"
    customers.write_text("customer_id\nc1\nc2\n", encoding="utf-8")
    inp=tmp_path/"events.csv"
    with open(inp,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=FIELDS); w.writeheader(); w.writerows(rows)
    return inp, customers

def row(event_id="e1", **overrides):
    result={"event_id":event_id,"order_id":"o1","customer_id":"c1","event_type":"paid","amount":"12.30","currency":"USD","event_timestamp":"2026-01-01T10:00:00Z"}
    result.update(overrides); return result

def run(tmp_path, inp, customers, **kwargs):
    return run_pipeline(inp, customers, tmp_path/"state.sqlite3", tmp_path/"output", chunk_size=2, **kwargs)

def test_happy_path_and_idempotency(tmp_path):
    inp, customers=setup_files(tmp_path,[row(),row("e2",event_timestamp="2026-01-02T00:00:00Z")])
    first=run(tmp_path,inp,customers)
    assert first["status"]=="success" and first["inserted_records"]==2
    assert first["contract_validation"]["compatible"] is True
    second=run(tmp_path,inp,customers)
    assert second["inserted_records"]==0 and second["duplicate_records"]==2
    with open(tmp_path/"output"/"processed"/"events.csv",encoding="utf-8") as f: assert len(list(csv.DictReader(f)))==2

def test_invalid_amount_null_reference_timestamp_and_event_type_quarantined(tmp_path):
    rows=[row("neg",amount="-1"),row("null",amount=""),row("ref",customer_id="unknown"),row("ts",event_timestamp="bad"),row("type",event_type="refunded")]
    inp,customers=setup_files(tmp_path,rows); report=run(tmp_path,inp,customers)
    assert report["rejected_records"]==5 and report["inserted_records"]==0
    reject=list((tmp_path/"output"/"rejected").glob("*_rejects.csv"))[0].read_text(encoding="utf-8")
    assert "amount must be non-negative" in reject and "unknown customer_id" in reject

def test_exact_duplicate_and_conflicting_duplicate(tmp_path):
    inp,customers=setup_files(tmp_path,[row("same"),row("same"),row("same",amount="99")])
    report=run(tmp_path,inp,customers)
    assert report["inserted_records"]==1 and report["duplicate_records"]==1 and report["rejected_records"]==1

def test_empty_input(tmp_path):
    inp,customers=setup_files(tmp_path,[]); report=run(tmp_path,inp,customers)
    assert report["status"]=="success" and report["input_records"]==0

def test_missing_required_field_quarantined_jsonl(tmp_path):
    customers=tmp_path/"customers.csv"; customers.write_text("customer_id\nc1\n",encoding="utf-8")
    inp=tmp_path/"events.jsonl"; inp.write_text(json.dumps({"event_id":"e1","customer_id":"c1"})+"\n{bad json\n",encoding="utf-8")
    report=run(tmp_path,inp,customers,input_format="jsonl")
    assert report["rejected_records"]==2 and report["accounting_check"] is True

def test_invalid_reference_and_negative_are_domain_edges(tmp_path):
    inp,customers=setup_files(tmp_path,[row("e1",amount="0"),row("e2",customer_id="ghost")])
    report=run(tmp_path,inp,customers)
    assert report["inserted_records"]==1 and report["rejected_records"]==1

def test_unexpected_schema_change_fails_explicitly(tmp_path):
    customers=tmp_path/"customers.csv"; customers.write_text("customer_id\nc1\n",encoding="utf-8")
    inp=tmp_path/"bad.csv"; inp.write_text("event_id,order_id\ne1,o1\n",encoding="utf-8")
    with pytest.raises(RuntimeError,match="unexpected schema"):
        run(tmp_path,inp,customers)

def test_partial_failure_then_retry_is_safe(tmp_path):
    inp,customers=setup_files(tmp_path,[row("e1"),row("e2"),row("e3")])
    with pytest.raises(RuntimeError,match="simulated failure"):
        run(tmp_path,inp,customers,fail_after=1)
    failed_reports=list((tmp_path/"output"/"reports").glob("*_report.json"))
    failed_report=json.loads(failed_reports[0].read_text(encoding="utf-8"))
    assert failed_report["status"]=="failed" and failed_report["accounting_check"] is True
    report=run(tmp_path,inp,customers)
    assert report["inserted_records"]==2 and report["duplicate_records"]==1
    assert report["target_total_records"]==3

def test_late_out_of_order_event_preserves_event_time(tmp_path):
    inp,customers=setup_files(tmp_path,[row("new",event_timestamp="2026-02-01T00:00:00Z"),row("late",event_timestamp="2025-12-31T23:59:00Z")])
    report=run(tmp_path,inp,customers)
    assert report["inserted_records"]==2
    with open(tmp_path/"output"/"processed"/"events.csv",encoding="utf-8") as f: records=list(csv.DictReader(f))
    assert records[0]["event_id"]=="late" and records[0]["event_timestamp"].startswith("2025-12-31")
