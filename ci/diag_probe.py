# ci/diag_probe.py (kept for reference if you want to run standalone)
# (Embedded version already runs from wfo_entry.py)
import argparse, glob, os, json, pandas as pd
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--limit", type=int, default=3)
    args = ap.parse_args()
    pat = os.path.join(args.data_root or ".", args.csv_glob)
    paths = sorted(glob.glob(pat, recursive=True))
    rep = {"pattern": pat, "n_files": len(paths), "samples": []}
    for p in paths[:args.limit]:
        try: df = pd.read_csv(p, nrows=5); rep["samples"].append({"path": p, "cols": list(df.columns)})
        except Exception as e: rep["samples"].append({"path": p, "error": str(e)})
    os.makedirs(args.outdir, exist_ok=True)
    with open(os.path.join(args.outdir,"diag_probe.json"),"w",encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    if len(paths)==0:
        print("[diag_probe] No CSV matched"); raise SystemExit(11)
if __name__=="__main__": main()
