# ci/precheck_contract.py
import argparse, os, glob, json, pandas as pd

OK, WARN, FAIL = "OK","WARN","FAIL"
def _exists(p): return os.path.exists(p)

def check_data(root, pattern):
    matches = glob.glob(os.path.join(root, pattern), recursive=True)
    if not matches: return FAIL, "no CSV matched"
    import pandas as pd
    df = pd.read_csv(matches[0], nrows=5)
    cols = {c.lower() for c in df.columns}
    need = {"close"}; dt_candidates = {"open_time","timestamp","time","datetime","date"}
    if "close" not in cols or not (cols & dt_candidates):
        return FAIL, f"data columns miss: have={sorted(cols)[:10]}"
    return OK, f"csv_ok: {os.path.basename(matches[0])}"

def check_trades(outdir):
    p = os.path.join(outdir, "trades.csv")
    if not _exists(p): return WARN, "trades.csv missing (will be empty)"
    import pandas as pd
    df = pd.read_csv(p, nrows=10)
    cols = {c.lower() for c in df.columns}
    need = {"open_time","event"}
    if not need.issubset(cols):
        return FAIL, f"trades miss: need {need}, have={cols}"
    return OK, f"trades_ok cols={list(cols)[:6]}"

def check_preds(outdir):
    p = os.path.join(outdir, "preds_test.csv")
    if not _exists(p): return WARN, "preds_test.csv missing (MCC skipped)"
    import pandas as pd
    df = pd.read_csv(p, nrows=5)
    cols = {c.lower() for c in df.columns}
    prob_cols = ["p","p_gate","gatep","prob","score","p_trend","p_range"]
    okprob = next((c for c in prob_cols if c in cols), None)
    if "open_time" not in cols or not okprob:
        return FAIL, f"preds miss: need open_time + one of {prob_cols}, have={cols}"
    return OK, f"preds_ok prob={okprob}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--outdir", required=True)
    a = ap.parse_args()
    r = {}
    r["data"]   = check_data(a.data_root, a.csv_glob)
    r["trades"] = check_trades(a.outdir)
    r["preds"]  = check_preds(a.outdir)
    print("[precheck]", r)
    # fail-fast 기준: data/trades FAIL 시 종료 13, preds FAIL 시 14
    if r["data"][0]==FAIL or r["trades"][0]==FAIL: raise SystemExit(13)
    if r["preds"][0]==FAIL: raise SystemExit(14)
if __name__=="__main__": main()