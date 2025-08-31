# ci/wfo_entry.py  (v5: codepack unzip + params overlay + runpy argv)
import argparse, glob, os, json, yaml, pathlib, datetime, sys, runpy, shutil

def load_params(p):
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def ensure_dir(d): pathlib.Path(d).mkdir(parents=True, exist_ok=True)

def pick_csvs(data_root, csv_glob):
    import glob as _g, os as _o
    pat = _o.path.join(data_root or ".", csv_glob)
    paths = sorted(_g.glob(pat, recursive=True))
    if not paths:
        raise SystemExit(f"No CSV matched: {pat}")
    return paths

def unzip_codepack_if_any(workspace="."):
    # find a likely strategy codepack at repo root
    cands = []
    for fn in os.listdir(workspace):
        if fn.endswith(".zip") and "strategy" in fn.lower():
            cands.append(os.path.join(workspace, fn))
    extracted = []
    for zp in cands:
        try:
            import zipfile
            with zipfile.ZipFile(zp,'r') as z:
                z.extractall(workspace)
                extracted.extend(z.namelist())
            print(f"[wfo_entry] extracted codepack: {os.path.basename(zp)} ({len(extracted)} files)")
        except Exception as e:
            print(f"[wfo_entry] skip codepack {zp}: {e}")
    return extracted

def overlay_params(cfg, thr=None, hold=None, filt=None):
    # Map WFO knobs to this strategy's params
    p = dict(cfg) if isinstance(cfg, dict) else {}
    # entry p_thr: set both trend/range if thr provided
    if thr is not None:
        ep = (p.get("entry") or {}).get("p_thr") or {}
        ep["trend"] = float(thr)
        ep["range"] = float(thr)
        p.setdefault("entry", {})["p_thr"] = ep
    # exit min_hold: set from hold if provided
    if hold is not None:
        ex = p.get("exit") or {}
        ex["min_hold"] = int(hold)
        p["exit"] = ex
    # 'filter' is not used by this code; record in meta only
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
    # search widely
    for root,dirs,files in os.walk(workspace):
        for fn in files:
            if fn.endswith(".py") and fn.lower() in {"runner.py","run_4u.py","run.py"}:
                return os.path.join(root, fn)
    raise FileNotFoundError("runner script not found after codepack extraction")

def run_script_with_argv(py_path, argv):
    old_argv = list(sys.argv)
    try:
        sys.argv = argv
        print("[wfo_entry] exec", py_path, "argv:", " ".join(argv[1:]))
        runpy.run_path(py_path, run_name="__main__")
    finally:
        sys.argv = old_argv

def write_minimum(outdir):
    ensure_dir(outdir)
    defaults = {
        "summary.json": "{}\n",
        "gating_debug.json": "{}\n",
        "preds_test.csv": "empty\n",
        "trades.csv": "empty\n",
    }
    for fn, body in defaults.items():
        fp = os.path.join(outdir, fn)
        if not os.path.exists(fp):
            with open(fp, "w", encoding="utf-8") as f: f.write(body)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", "--config", dest="params", required=True)
    ap.add_argument("--data-root", default=".")
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--thr", type=float)
    ap.add_argument("--hold", type=int)
    ap.add_argument("--filter", type=str)
    ap.add_argument("--outdir", required=True)
    # optional explicit runner path
    ap.add_argument("--runner")
    args = ap.parse_args()

    # 0) Extract strategy codepack if present
    unzip_codepack_if_any(os.getcwd())

    # 1) prepare CSVs early (validation)
    csvs = pick_csvs(args.data_root, args.csv_glob)

    # 2) load and overlay params → write to outdir/params_used.yml
    base_cfg = load_params(args.params)
    patched_cfg = overlay_params(base_cfg, args.thr, args.hold, args.filter)
    params_path = write_params_file(patched_cfg, args.outdir)

    # 3) locate runner
    runner = args.runner or find_runner_path(os.getcwd())

    # 4) run script via runpy with argv for argparse
    argv = [
        runner,
        "--data-root", args.data_root,
        "--csv-glob", args.csv_glob,
        "--outdir", args.outdir,
        "--params", params_path,
    ]
    run_script_with_argv(runner, argv)

    # 5) ensure minimum artifacts
    write_minimum(args.outdir)

    # 6) manifest
    with open(os.path.join(args.outdir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({
            "thr": args.thr, "hold": args.hold, "filter": args.filter,
            "csv_glob": args.csv_glob, "data_root": args.data_root,
            "params_file": params_path, "runner": runner,
            "ts": datetime.datetime.utcnow().isoformat()+"Z"
        }, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
