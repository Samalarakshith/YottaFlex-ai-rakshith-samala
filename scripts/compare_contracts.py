"""Compare two versioned contract JSON files and emit compatibility JSON."""
import argparse, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from streametl.contracts import load_contract, compare_contracts

p = argparse.ArgumentParser(description="Classify breaking and non-breaking data contract changes")
p.add_argument("--old", required=True, help="Existing consumer contract JSON")
p.add_argument("--new", required=True, help="Proposed producer contract JSON")
p.add_argument("--report", help="Optional path to save JSON report")
a = p.parse_args()
report = compare_contracts(load_contract(a.old), load_contract(a.new))
text = json.dumps(report, indent=2)
print(text)
if a.report:
    Path(a.report).parent.mkdir(parents=True, exist_ok=True)
    Path(a.report).write_text(text + "\n", encoding="utf-8")
sys.exit(0 if report["compatible"] else 2)
