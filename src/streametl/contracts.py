"""Data-contract loading, producer-batch schema checks, and compatibility diffs."""
from __future__ import annotations
import csv, json
from pathlib import Path
from typing import Any


def load_contract(path: str | Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        contract = json.load(f)
    if not contract.get("contract_name") or not isinstance(contract.get("fields"), dict):
        raise ValueError("contract must define contract_name and fields")
    for name, spec in contract["fields"].items():
        if spec.get("type") not in {"string", "decimal", "timestamp"}:
            raise ValueError(f"unsupported contract type for {name}: {spec.get('type')}")
        if "required" not in spec or "nullable" not in spec:
            raise ValueError(f"field {name} must define required and nullable")
    return contract


def compare_contracts(old: dict, new: dict) -> dict:
    """Compare old consumer contract to a proposed new contract (backward compatibility)."""
    changes = []
    old_fields, new_fields = old["fields"], new["fields"]
    for name, spec in old_fields.items():
        if name not in new_fields:
            changes.append({"field": name, "change": "field_removed", "severity": "breaking", "reason": "Existing field removed"})
            continue
        nxt = new_fields[name]
        if spec["type"] != nxt["type"]:
            changes.append({"field": name, "change": "type_changed", "severity": "breaking", "reason": f"Type changed from {spec['type']} to {nxt['type']}"})
        if spec["nullable"] != nxt["nullable"]:
            direction = "relaxed" if nxt["nullable"] else "tightened"
            changes.append({"field": name, "change": f"nullability_{direction}", "severity": "breaking", "reason": "Changing nullability can invalidate existing producers or consumers"})
        if spec["required"] != nxt["required"]:
            direction = "relaxed" if not nxt["required"] else "tightened"
            changes.append({"field": name, "change": f"requiredness_{direction}", "severity": "breaking", "reason": "Changing requiredness can invalidate existing producers or consumers"})
        old_values, new_values = set(spec.get("allowed_values", [])), set(nxt.get("allowed_values", []))
        if old_values and not old_values.issubset(new_values):
            changes.append({"field": name, "change": "allowed_values_removed", "severity": "breaking", "reason": f"Removed allowed values: {sorted(old_values-new_values)}"})
        elif new_values - old_values and old_values:
            changes.append({"field": name, "change": "allowed_values_added", "severity": "non_breaking", "reason": f"Added allowed values: {sorted(new_values-old_values)}"})
    for name, spec in new_fields.items():
        if name not in old_fields:
            severity = "non_breaking" if not spec["required"] or spec["nullable"] or "default" in spec else "breaking"
            changes.append({"field": name, "change": "field_added", "severity": severity,
                            "reason": "Optional/nullable field added" if severity == "non_breaking" else "New required field has no default"})
    breaking = any(c["severity"] == "breaking" for c in changes)
    return {"compatibility_policy": "backward", "compatible": not breaking,
            "classification": "breaking" if breaking else ("non_breaking" if changes else "no_change"), "changes": changes}


def inspect_batch_schema(input_path: str | Path, input_format: str = "auto") -> dict:
    path = Path(input_path)
    fmt = input_format.lower()
    if fmt == "auto": fmt = "jsonl" if path.suffix.lower() in {".jsonl", ".ndjson"} else "csv"
    if fmt == "csv":
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            columns = next(reader, [])
    elif fmt == "jsonl":
        columns = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                try: item = json.loads(line)
                except json.JSONDecodeError: continue
                if isinstance(item, dict):
                    columns = list(item.keys()); break
    else: raise ValueError("input format must be csv or jsonl")
    return {"format": fmt, "columns": [str(c).strip().lower() for c in columns], "empty_batch": not bool(columns)}


def validate_batch_schema(observed: dict, contract: dict) -> dict:
    columns = set(observed["columns"])
    fields = contract["fields"]
    if observed.get("empty_batch"):
        return {"contract_name": contract["contract_name"], "contract_version": contract.get("version", 1),
                "compatibility_policy": contract.get("compatibility_policy", "backward"), "compatible": True,
                "classification": "no_change", "observed_columns": [], "missing_required_fields": [],
                "additional_fields": [], "changes": [], "empty_batch": True}
    missing = sorted(name for name, spec in fields.items() if spec.get("required", False) and name not in columns)
    extras = sorted(columns - set(fields))
    changes = []
    for name in missing:
        changes.append({"field": name, "change": "required_field_missing", "severity": "breaking", "reason": "Required contract field missing from producer batch"})
    for name in extras:
        changes.append({"field": name, "change": "unknown_field_added", "severity": "non_breaking", "reason": "Additive producer field is accepted and ignored unless contract is updated"})
    breaking = bool(missing)
    return {"contract_name": contract["contract_name"], "contract_version": contract.get("version", 1),
            "compatibility_policy": contract.get("compatibility_policy", "backward"),
            "compatible": not breaking, "classification": "breaking" if breaking else ("non_breaking" if extras else "no_change"),
            "observed_columns": observed["columns"], "missing_required_fields": missing,
            "additional_fields": extras, "changes": changes}


def validate_record_contract(record: dict, contract: dict) -> None:
    """Raise ValueError for contract-level nullability, type, or allowed-value violations."""
    for name, spec in contract["fields"].items():
        value = record.get(name)
        empty = value is None or (isinstance(value, str) and not value.strip())
        if empty:
            if spec.get("required") and not spec.get("nullable"):
                raise ValueError(f"contract: {name} is required and non-nullable")
            continue
        typ = spec["type"]
        try:
            if typ == "decimal":
                from decimal import Decimal, InvalidOperation
                value_dec = Decimal(str(value))
                if not value_dec.is_finite(): raise InvalidOperation()
                if "minimum" in spec and value_dec < Decimal(str(spec["minimum"])):
                    raise ValueError(f"contract: {name} must be >= {spec['minimum']}")
            elif typ == "timestamp":
                from datetime import datetime
                datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            elif typ == "string" and isinstance(value, (dict, list)):
                raise ValueError(f"contract: {name} must be a string")
        except (ValueError, TypeError) as exc:
            if str(exc).startswith("contract:"): raise
            raise ValueError(f"contract: {name} must be a valid {typ}") from exc
        if "allowed_values" in spec and str(value) not in spec["allowed_values"]:
            raise ValueError(f"contract: {name} must be one of: {', '.join(spec['allowed_values'])}")
