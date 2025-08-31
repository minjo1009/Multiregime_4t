# ci/post_sanity.py (kept for reference; embedded version is run automatically)
import argparse, os, json, pandas as pd
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    s=os.path.join(args.outdir,"summary.json"); t=os.path.join(args.outdir,"trades.csv")
    summ = json.load(open(s,"r",encoding="utf-8")) if os.path.exists(s) else {}
    exits = summ.get("exits")
    n_rows = len(pd.read_csv(t)) if os.path.exists(t) else None
    with open(os.path.join(args.outdir,"post_sanity.json"),"w",encoding="utf-8") as f:
        json.dump({"exits":exits,"trades_rows":n_rows}, f, ensure_ascii=False, indent=2)
    if (exits is None or exits==0) and (n_rows is None or n_rows==0):
        raise SystemExit(12)
if __name__=="__main__": main()
