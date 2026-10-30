import json
from pathlib import Path
import pytest
from streametl.contracts import load_contract, compare_contracts, inspect_batch_schema, validate_batch_schema, validate_record_contract

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"


def test_optional_nullable_field_is_non_breaking():
    result = compare_contracts(load_contract(CONTRACTS / "events_v1.json"), load_contract(CONTRACTS / "events_v2_optional_region.json"))
    assert result["compatible"] is True
    assert result["classification"] == "non_breaking"
    assert any(c["field"] == "region" and c["severity"] == "non_breaking" for c in result["changes"])


def test_removed_key_and_type_change_are_breaking():
    result = compare_contracts(load_contract(CONTRACTS / "events_v1.json"), load_contract(CONTRACTS / "events_v2_breaking.json"))
    assert result["compatible"] is False
    assert result["classification"] == "breaking"
    assert {c["field"] for c in result["changes"] if c["severity"] == "breaking"} >= {"event_id", "amount"}


def test_batch_schema_detects_missing_required_and_extra_fields(tmp_path):
    path = tmp_path / "producer.csv"
    path.write_text("event_id,amount,extra\ne1,1.0,x\n", encoding="utf-8")
    contract = load_contract(CONTRACTS / "events_v1.json")
    observed = inspect_batch_schema(path)
    report = validate_batch_schema(observed, contract)
    assert report["compatible"] is False
    assert "customer_id" in report["missing_required_fields"]
    assert report["additional_fields"] == ["extra"]


def test_record_contract_checks_nullability_type_minimum_and_enum():
    contract = load_contract(CONTRACTS / "events_v1.json")
    good = {"event_id":"e1", "order_id":"o1", "customer_id":"c1", "event_type":"paid", "amount":"12.30", "currency":"USD", "event_timestamp":"2026-01-01T10:00:00Z"}
    validate_record_contract(good, contract)
    with pytest.raises(ValueError, match="amount must be >= 0"):
        validate_record_contract({**good, "amount":"-1"}, contract)
    with pytest.raises(ValueError, match="event_type must be one of"):
        validate_record_contract({**good, "event_type":"unknown"}, contract)
    with pytest.raises(ValueError, match="event_id is required"):
        validate_record_contract({**good, "event_id":""}, contract)
