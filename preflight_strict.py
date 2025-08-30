import argparse, json, os, glob
import pandas as pd

REQUIRED = ["open_time","open","high","low","close","volume"]

def find_csv(data_root: str, csv_glob: str):
    pattern = csv_glob if csv_glob else "**/*.csv"
    paths = glob.glob(os.path.join(data_root, pattern), recursive=True)
    if not paths:
        raise FileNotFoundError(f"No CSV found under {data_root} with pattern {pattern}")
    # Prefer ETHUSDT-like if multiple
    preferred = [p for p in paths if "ETHUSDT" in os.path.basename(p).upper()]
    return (preferred or paths)[0]

def ensure_out(outdir: str):
    os.makedirs(outdir, exist_ok=True)
    gd = os.path.join(outdir, "gating_debug.json")
    if not os.path.exists(gd):
        with open(gd, "w") as f:
            f.write("{}")
    preds = os.path.join(outdir, "preds_test.csv")
    if not os.path.exists(preds):
        with open(preds, "w") as f:
            f.write("open_time,signal,score\n")
    trades = os.path.join(outdir, "trades.csv")
    if not os.path.exists(trades):
        with open(trades, "w") as f:
            f.write("open_time,side,price,qty\n")
    return gd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--csv-glob", default="**/*.csv")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    ensure_out(args.outdir)
    csv_path = find_csv(args.data_root, args.csv_glob)
    df = pd.read_csv(csv_path, nrows=2000)
    missing = [c for c in REQUIRED if c not in df.columns]
    report = {
        "csv_path": csv_path,
        "rows_read": len(df),
        "missing": missing,
        "required": REQUIRED
    }
    if missing:
        report["status"] = "fail"
    else:
        report["status"] = "ok"
    with open(os.path.join(args.outdir, "summary.json"), "w") as f:
        json.dump(report, f, indent=2)
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

if __name__ == "__main__":
    main()
