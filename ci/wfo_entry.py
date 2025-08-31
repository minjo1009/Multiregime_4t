# ci/wfo_entry.py  (v7.2: unzip + overlay + runpy + diag pre/post + enrich)
import argparse, os, json, yaml, pathlib, sys, runpy, datetime

try:
    import pandas as pd  # used by post_enrich
    import numpy as np
except Exception:
    pd = None
    np = None

def ensure_dir(d): pathlib.Path(d).mkdir(parents=True, exist_ok=True)

def load_params(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def pick_csvs(data_root, csv_glob):
    import glob as _g
    pat = os.path.join(data_root or ".", csv_glob)
    paths = sorted(_g.glob(pat, recursive=True))
    return pat, paths

def unzip_codepack_if_any(workspace="."):
    cands = []
    for fn in os.listdir(workspace):
        if fn.endswith(".zip") and "strategy" in fn.lower():
            cands.append(os.path.join(workspace, fn))
    for zp in cands:
        try:
            import zipfile
            with zipfile.ZipFile(zp,'r') as z: z.extractall(workspace)
            print(f"[wfo_entry] extracted codepack: {os.path.basename(zp)}")
        except Exception as e:
            print(f"[wfo_entry] skip codepack {zp}: {e}")
    # ensure packages
    for pkg in ["strategy", os.path.join("strategy","v2"), "backtest"]:
        p = os.path.join(workspace, pkg)
        if os.path.isdir(p):
            ip = os.path.join(p, "__init__.py")
            if not os.path.exists(ip):
                try: open(ip, "w", encoding="utf-8").write("# auto-generated\n")
                except: pass

def overlay_params(cfg, thr=None, hold=None, filt=None):
    p = dict(cfg) if isinstance(cfg, dict) else {}
    if thr is not None:
        ep = (p.get("entry") or {}).get("p_thr") or {}
        ep["trend"] = float(thr)
        ep["range"] = float(thr)
        p.setdefault("entry", {})["p_thr"] = ep
    if hold is not None:
        ex = p.get("exit") or {}
        ex["min_hold"] = int(hold)
        p["exit"] = ex
    # filter is meta only for this codebase
    return p

def write_params_file(p, outdir):
    ensure_dir(outdir)
    outp = os.path.join(outdir, "params_used.yml")
    with open(outp, "w", encoding="utf-8") as f:
        yaml.safe_dump(p, f, sort_keys=False, allow_unicode=True)
    return outp

def find_runner_path(workspace="."):
    pref = ["backtest/runner.py", "run_4u.py", "runner.py", "run.py"]
    for rel in pref:
        cand = os.path.join(workspace, rel)
        if os.path.isfile(cand):
            return cand
    for root,dirs,files in os.walk(workspace):
        for fn in files:
            if fn.endswith(".py") and fn.lower() in {"runner.py","run_4u.py","run.py"}:
                return os.path.join(root, fn)
    raise FileNotFoundError("runner script not found after codepack extraction")

def run_script_with_argv(py_path, argv):
    old_argv = list(sys.argv)
    try:
        ws = os.getcwd(); sd = os.path.dirname(py_path)
        for p in [os.path.join(ws,"src"), os.path.join(ws,"backtest"), ws, sd]:
            if p and p not in sys.path: sys.path.insert(0, p)
        print("[wfo_entry] sys.path[0:4] =", sys.path[:4])
        sys.argv = argv
        print("[wfo_entry] exec", py_path, "argv:", " ".join(argv[1:]))
        runpy.run_path(py_path, run_name="__main__")
    finally:
        sys.argv = old_argv

# ---------- diagnostics & enrich ----------
def diag_probe(data_root, csv_glob, outdir, limit=3):
    import glob, pandas as pd
    pat = os.path.join(data_root or ".", csv_glob)
    paths = sorted(glob.glob(pat, recursive=True))
    rep = {"pattern": pat, "n_files": len(paths), "samples": []}
    for p in paths[:limit]:
        try:
            df = pd.read_csv(p, nrows=5)
            rep["samples"].append({"path": p, "cols": list(df.columns), "head": df.head(3).to_dict("records")})
        except Exception as e:
            rep["samples"].append({"path": p, "error": str(e)})
    with open(os.path.join(outdir, "diag_probe.json"), "w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    if len(paths)==0:
        print("[diag_probe] No CSV matched:", pat)
        raise SystemExit(11)
    print("[diag_probe] files matched:", len(paths))

def post_enrich(outdir):
    import pandas as pd, numpy as np, math, json, os
    PNL_CANDS = ['pnl_close_based','pnl','pnl_value','pnl_usd','pnl_krw','pnl_pct','pnl_percent','ret','return','pnl_close']
    def _read_json(p):
        try: return json.load(open(p,"r",encoding="utf-8"))
        except: return {}
    def _read_csv(p):
        try: return pd.read_csv(p)
        except: 
            try: return pd.read_csv(p, engine="python", sep=",", on_bad_lines="skip")
            except: return pd.DataFrame()
    def _count_exits(df):
        if df is None or df.empty: return 0
        ev = next((c for c in df.columns if str(c).lower()=="event"), None)
        if ev: return int((df[ev].astype(str).str.upper()=="EXIT").sum())
        return int(len(df)//2)
    def _pnl_series(df):
        if df is None or df.empty: return None
        ev = next((c for c in df.columns if str(c).lower()=="event"), None)
        if ev: df = df[df[ev].astype(str).str.upper()=="EXIT"]
        for c in PNL_CANDS:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
                if len(s): return s
        return None
    def _mcc(preds):
        if preds is None or preds.empty: return None
        m = {str(c).lower():c for c in preds.columns}
        yt = next((m.get(k) for k in ['y_true','true','label','target'] if k in m), None)
        yp = next((m.get(k) for k in ['y_pred','pred','prediction','pred_label'] if k in m), None)
        if yt is None or yp is None: return None
        Yt = preds[yt].values; Yp = preds[yp].values
        if preds[yp].dtype.kind in 'f': Yp = (Yp>=0.5).astype(int)
        tp=int(((Yt==1)&(Yp==1)).sum()); tn=int(((Yt==0)&(Yp==0)).sum())
        fp=int(((Yt==0)&(Yp==1)).sum()); fn=int(((Yt==1)&(Yp==0)).sum())
        den=(tp+fp)*(tp+fn)*(tn+fp)*(tn+fn); den=math.sqrt(den) if den else 0
        return float((tp*tn - fp*fn)/den) if den else float('nan')
    trades_p = os.path.join(outdir,"trades.csv")
    preds_p  = os.path.join(outdir,"preds_test.csv")
    summ_p   = os.path.join(outdir,"summary.json")
    trades = _read_csv(trades_p) if os.path.exists(trades_p) else pd.DataFrame()
    preds  = _read_csv(preds_p) if os.path.exists(preds_p) else pd.DataFrame()
    summ   = _read_json(summ_p)
    n_exits = summ.get("exits") or _count_exits(trades); summ["exits"] = n_exits
    summ.setdefault("entries", n_exits)
    s = _pnl_series(trades)
    if s is not None:
        wr = float((s>0).sum())/max(1,len(s))
        pos=float(s[s>0].sum()); neg=float(s[s<0].sum())
        pf = (pos/abs(neg)) if neg!=0 else float('nan')
        cp = float(s.sum())
        summ.setdefault("win_rate", wr); summ["win_rate_from_trades"]=wr
        summ.setdefault("profit_factor", pf); summ["profit_factor_from_trades"]=pf
        summ.setdefault("cum_pnl_close_based", cp)
    mcc = _mcc(preds)
    if mcc is not None: summ["mcc"]=mcc
    with open(summ_p,"w",encoding="utf-8") as f: json.dump(summ, f, ensure_ascii=False, indent=2)
    print("[post_enrich] exits:", n_exits, "summary keys:", list(summ.keys())[:8])

def post_sanity(outdir):
    summ_p=os.path.join(outdir,"summary.json"); trades_p=os.path.join(outdir,"trades.csv")
    summ = {}
    if os.path.exists(summ_p):
        try: summ=json.load(open(summ_p,"r",encoding="utf-8"))
        except: summ={}
    exits = summ.get("exits")
    n_rows = None
    if os.path.exists(trades_p):
        try:
            import pandas as pd
            n_rows = len(pd.read_csv(trades_p))
        except Exception as e:
            print("[post_sanity] trades.csv read error:", e)
    rep = {"exits": exits, "trades_rows": n_rows}
    with open(os.path.join(outdir,"post_sanity.json"),"w",encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)
    if (exits is None or exits==0) and (n_rows is None or n_rows==0):
        raise SystemExit(12)
    print("[post_sanity] OK:", rep)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", "--config", dest="params", required=True)
    ap.add_argument("--data-root", default=".")
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--thr", type=float)
    ap.add_argument("--hold", type=int)
    ap.add_argument("--filter", type=str)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--runner")
    args = ap.parse_args()

    unzip_codepack_if_any(os.getcwd())

    pat, paths = pick_csvs(args.data_root, args.csv_glob)
    print(f"[wfo_entry] data pattern: {pat} (matched {len(paths)})")
    # pre-probe
    ensure_dir(args.outdir)
    diag_probe(args.data_root, args.csv_glob, args.outdir)

    base_cfg = load_params(args.params)
    patched = overlay_params(base_cfg, args.thr, args.hold, args.filter)
    params_path = write_params_file(patched, args.outdir)

    runner = args.runner or find_runner_path(os.getcwd())
    argv = [runner, "--data-root", args.data_root, "--csv-glob", args.csv_glob, "--outdir", args.outdir, "--params", params_path]
    run_script_with_argv(runner, argv)

    # enrich & sanity
    post_enrich(args.outdir)
    post_sanity(args.outdir)

    # manifest
    with open(os.path.join(args.outdir,"manifest.json"),"w",encoding="utf-8") as f:
        json.dump({
            "thr": args.thr, "hold": args.hold, "filter": args.filter,
            "csv_glob": args.csv_glob, "data_root": args.data_root,
            "params_file": params_path, "runner": runner,
            "ts": datetime.datetime.utcnow().isoformat()+"Z"
        }, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
