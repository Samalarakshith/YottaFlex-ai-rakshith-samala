import argparse, json
from .pipeline import run_pipeline

def main():
    parser = argparse.ArgumentParser(description="Memory-efficient CSV/JSONL ETL pipeline")
    parser.add_argument("--input", required=True)
    parser.add_argument("--customers", required=True)
    parser.add_argument("--db", default="output/streametl.sqlite3")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--format", choices=["auto", "csv", "jsonl"], default="auto")
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--contract", default=None, help="Path to versioned JSON data contract (default: contracts/events_v1.json)")
    args = parser.parse_args()
    report = run_pipeline(args.input, args.customers, args.db, args.output_dir, args.format, args.chunk_size, contract_path=args.contract)
    print(json.dumps(report, indent=2))
if __name__ == "__main__": main()
