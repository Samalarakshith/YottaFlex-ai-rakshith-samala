"""Incremental readers: pandas chunks for CSV and line-by-line JSONL."""
import json
from pathlib import Path
from typing import Iterator
import pandas as pd


def read_csv_chunks(path: str | Path, chunk_size: int) -> Iterator[tuple[int, dict]]:
    """Yield (source row number, record) without materializing the whole CSV."""
    # pandas handles CSV in bounded chunks. Keep raw strings for deterministic validation.
    offset = 1  # header is row 1; first data row is row 2
    for chunk in pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=chunk_size, on_bad_lines="error"):
        for record in chunk.to_dict(orient="records"):
            offset += 1
            yield offset, record


def read_jsonl(path: str | Path) -> Iterator[tuple[int, dict | Exception]]:
    """Yield one JSONL record at a time; malformed lines are yielded as exceptions."""
    with open(path, "r", encoding="utf-8") as handle:
        for row_num, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise ValueError("JSONL record must be an object")
                yield row_num, item
            except (json.JSONDecodeError, ValueError) as exc:
                yield row_num, exc
