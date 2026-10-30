"""Deterministic validation and canonicalization."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from .config import REQUIRED_COLUMNS, ALLOWED_EVENT_TYPES, SUPPORTED_CURRENCIES


def normalize_timestamp(value: str) -> str:
    raw = str(value).strip()
    if not raw:
        raise ValueError("event_timestamp is required")
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_timestamp must be ISO-8601") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # documented assumption: naive timestamps mean UTC
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_record(record: dict, customer_ids: set[str]) -> dict:
    missing_columns = [c for c in REQUIRED_COLUMNS if c not in record]
    if missing_columns:
        raise ValueError("missing required column(s): " + ", ".join(missing_columns))
    normalized = {str(k).strip().lower(): ("" if v is None else str(v).strip()) for k, v in record.items()}
    for col in REQUIRED_COLUMNS:
        if not normalized.get(col, ""):
            raise ValueError(f"{col} is required")
    if normalized["event_type"] not in ALLOWED_EVENT_TYPES:
        raise ValueError("event_type must be one of: " + ", ".join(sorted(ALLOWED_EVENT_TYPES)))
    currency = normalized["currency"].upper()
    if currency not in SUPPORTED_CURRENCIES:
        raise ValueError("unsupported currency: " + currency)
    try:
        amount = Decimal(normalized["amount"])
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("amount must be a valid decimal") from exc
    if not amount.is_finite():
        raise ValueError("amount must be finite")
    if amount < 0:
        raise ValueError("amount must be non-negative")
    if normalized["customer_id"] not in customer_ids:
        raise ValueError("unknown customer_id reference")
    ts = normalize_timestamp(normalized["event_timestamp"])
    normalized["amount"] = format(amount.quantize(Decimal("0.01")), ".2f")
    normalized["currency"] = currency
    normalized["event_timestamp"] = ts
    normalized["event_date"] = ts[:10]
    return normalized


def payload_hash(record: dict) -> str:
    payload = {k: record[k] for k in sorted(record) if k not in {"source_file", "source_row", "run_id", "event_date"}}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(canonical.encode("utf-8")).hexdigest()
