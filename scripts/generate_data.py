"""Generate reproducible synthetic customers and order events."""
import argparse, csv, json, random
from datetime import datetime, timedelta, timezone
from pathlib import Path

def generate(out_dir: str, records: int = 1000, seed: int = 20261030):
    rng = random.Random(seed); base = Path(out_dir); base.mkdir(parents=True, exist_ok=True)
    customers = [f"cust_{i:06d}" for i in range(max(100, min(records // 5 + 1, 100000)))]
    with open(base / "customers.csv", "w", newline="", encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["customer_id"]); w.writerows([[c] for c in customers])
    fields=["event_id","order_id","customer_id","event_type","amount","currency","event_timestamp"]
    valid=[]; start=datetime(2026,1,1,tzinfo=timezone.utc)
    for i in range(records):
        dt=start+timedelta(seconds=rng.randint(0, 60*60*24*90))
        row={"event_id":f"evt_{i:09d}","order_id":f"ord_{i//2:09d}","customer_id":rng.choice(customers),"event_type":rng.choice(["created","paid","shipped","cancelled"]),"amount":f"{rng.uniform(0,1000):.2f}","currency":"USD","event_timestamp":dt.isoformat().replace("+00:00","Z")}
        valid.append(row)
    # Seed deterministic quality cases in first rows, without changing the core scale.
    if records >= 8:
        valid[0]["amount"] = "-5.00"                       # domain invalid
        valid[1]["customer_id"] = "cust_does_not_exist"   # invalid reference
        valid[2]["event_timestamp"] = "not-a-date"        # malformed timestamp
        valid[3]["event_id"] = ""                         # missing key
        valid[4]["amount"] = ""                            # null amount
        valid[5]["event_type"] = "refunded"                # unexpected category
        valid[6]["event_id"] = valid[7]["event_id"]        # duplicate/conflicting event ID
    with open(base / "orders.csv", "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(valid)
    with open(base / "orders.jsonl", "w", encoding="utf-8") as f:
        for row in valid: f.write(json.dumps(row, separators=(",",":"))+"\n")
    expected={"seed":seed,"requested_records":records,"customers":len(customers),"injected_cases":{"negative_amount":1 if records>=8 else 0,"unknown_customer":1 if records>=8 else 0,"invalid_timestamp":1 if records>=8 else 0,"missing_event_id":1 if records>=8 else 0,"missing_amount":1 if records>=8 else 0,"invalid_event_type":1 if records>=8 else 0,"duplicate_event_id":1 if records>=8 else 0},"note":"Expected labels are separate from input data; generator outputs are synthetic."}
    (base / "expected.json").write_text(json.dumps(expected,indent=2),encoding="utf-8")
    print(f"Generated {records} event rows in {base}; fixed seed={seed}")

if __name__ == "__main__":
    p=argparse.ArgumentParser(); p.add_argument("--out-dir",default="data/sample"); p.add_argument("--records",type=int,default=1000); p.add_argument("--seed",type=int,default=20261030); a=p.parse_args(); generate(a.out_dir,a.records,a.seed)
