# ci/sensitivity_vec.py
import argparse, os, sys, runpy, zipfile, json, yaml, glob, shutil
from pathlib import Path
import pandas as pd
import numpy as np

def sanitize_glob(g, repo_root):
    g = g.lstrip("./")
    rootname = os.path.basename(repo_root.rstrip("/"))
    if g.startswith(rootname + "/"):
        g = g[len(rootname)+1:]
    # strip 'Multiregime 4t*' prefix if present
    while True:
        parts = g.split("/", 1)
        if len(parts) == 2 and parts[0].lower().replace("_"," ").startswith("multiregime 4t"):
            g = parts[1]
        else:
            break
    return g

def ensure_codepack(codepack_zip, workdir):
    if not codepack_zip or not os.path.exists(codepack_zip):
        return None
    out = os.path.join(workdir, "_codepack")
    if os.path.exists(out):
        shutil.rmtree(out)
    with zipfile.ZipFile(codepack_zip) as z:
        z.extractall(out)
    return out

def patch_params(base_params_path, out_path, thr, hold):
    d = yaml.safe_load(open(base_params_path, "r", encoding="utf-8")) or {}
    d.setdefault("entry", {}).setdefault("p_thr", {})
    d["entry"]["p_thr"]["trend"] = float(thr)
    d["entry"]["p_thr"]["range"] = float(thr)
    d.setdefault("exit", {})
    d["exit"]["min_hold"] = int(hold)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    yaml.safe_dump(d, open(out_path, "w", encoding="utf-8"), sort_keys=False, allow_unicode=True)

def post_enrich(outdir):
    tp = os.path.join(outdir, "trades.csv")
    sp = os.path.join(outdir, "summary.json")
    if not os.path.exists(tp):
        return
    df = pd.read_csv(tp)
    ev = next((c for c in df.columns if str(c).lower()=="event"), None)
    if ev is not None:
        df = df[df[ev].astype(str).str.upper()=="EXIT"].copy()
    pnl = None
    for c in ["pnl_close_based","pnl","pnl_value","pnl_usd","pnl_krw","pnl_pct","pnl_percent","ret","return","pnl_close"]:
        if c in df.columns:
            pnl = c; break
    if pnl is None:
        df["pnl_close_based"] = 0.0
        pnl = "pnl_close_based"
        df.to_csv(tp, index=False)
    s = pd.to_numeric(df[pnl], errors="coerce").fillna(0.0)
    summ = {}
    if os.path.exists(sp):
        summ = json.load(open(sp, "r", encoding="utf-8"))
    summ["exits"] = int(len(s))
    summ["win_rate"] = float((s>0).sum())/max(1,len(s))
    pos, neg = float(s[s>0].sum()), float(s[s<0].sum())
    summ["profit_factor"] = (pos/abs(neg)) if neg!=0 else None
    summ["cum_pnl_close_based"] = float(s.sum())
    json.dump(summ, open(sp,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

def zip_dir(path, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in Path(path).rglob("*"):
            z.write(p, p.relative_to(path))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--thr-list", nargs="+", required=True)     # e.g. 0.81 0.83 0.85
    ap.add_argument("--hold-list", nargs="+", required=True)    # e.g. 8 9 10
    ap.add_argument("--codepack", default="strategy_v2_codepack_v2.1.3.zip")
    ap.add_argument("--runner", default="")
    ap.add_argument("--out-bundle", default="")
    args = ap.parse_args()

    repo_root = os.getcwd()
    csvg = sanitize_glob(args.csv_glob, repo_root)

    # prepare codepack
    cp_dir = ensure_codepack(args.codepack, repo_root)
    if args.runner:
        runner_path = args.runner
    else:
        runner_path = os.path.join(cp_dir or ".", "backtest", "runner.py")
        if not os.path.exists(runner_path):
            # fallback to repo runner
            runner_path = os.path.join("backtest", "runner.py")
    if not os.path.exists(runner_path):
        raise FileNotFoundError(f"runner not found: {runner_path}")

    # pythonpath
    sys.path[:0] = [repo_root]
    if cp_dir:
        sys.path[:0] = [cp_dir, os.path.join(cp_dir, "backtest")]

    # Warm-up: jit compile on first run (center combo if possible)
    thr0 = args.thr-list[len(args.thr_list)//2]
    hold0 = args.hold_list[len(args.hold_list)//2]
    # but we'll just iterate sequentially; numba will compile once

    out_zips = []
    for thr in args.thr_list:
        for hold in args.hold_list:
            outdir = f"out_thr{thr}_h{hold}"
            pfile = f"conf/params_thr{thr}_h{hold}.yml"
            patch_params(args.params, pfile, thr, hold)

            # build argv for runner.py
            saved_argv = sys.argv[:]
            sys.argv = [runner_path,
                        "--data-root", args.data_root,
                        "--csv-glob", csvg,
                        "--params", pfile,
                        "--outdir", outdir]
            try:
                runpy.run_path(runner_path, run_name="__main__")
            finally:
                sys.argv = saved_argv

            post_enrich(outdir)
            zpath = f"sweep_{thr}_{hold}.zip"
            zip_dir(outdir, zpath)
            out_zips.append(zpath)

    bundle = args.out_bundle or f"sweep_vec_bundle_{os.environ.get('GITHUB_SHA','local')[:7]}.zip"
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as z:
        for zp in out_zips:
            z.write(zp, os.path.basename(zp))
    print(f"[bundle] {bundle} ({len(out_zips)} items)")

if __name__ == "__main__":
    main()