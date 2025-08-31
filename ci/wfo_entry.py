# ci/wfo_entry.py
import argparse, glob, os, json, importlib, yaml, pathlib, datetime, sys

def load_params(p):
    with open(p, "r", encoding="utf-8") as f: 
        return yaml.safe_load(f) or {}

def pick_csvs(data_root, csv_glob):
    pat = os.path.join(data_root or ".", csv_glob)
    paths = sorted(glob.glob(pat, recursive=True))
    if not paths:
        raise FileNotFoundError(f"No CSV matched: {pat}")
    return paths

def ensure_dir(d): pathlib.Path(d).mkdir(parents=True, exist_ok=True)

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
    args = ap.parse_args()

    cfg = load_params(args.params)
    entry = (cfg.get("entry") or {})
    module = entry.get("module", "backtest.runner")
    fn_name = entry.get("fn", "run_once")

    csvs = pick_csvs(args.data_root, args.csv_glob)
    ensure_dir(args.outdir)

    # 전략 호출
    mod = importlib.import_module(module)
    fn = getattr(mod, fn_name)

    kwargs = dict(
        csv_paths=csvs, params=cfg, outdir=args.outdir,
        thr=args.thr, hold=args.hold, filter_name=args.filter
    )
    try:
        fn(**kwargs)
    except TypeError:
        fn(csv_paths=csvs, params=cfg, outdir=args.outdir)

    # 최소 산출물 보장
    write_minimum(args.outdir)

    # 메타 기록
    with open(os.path.join(args.outdir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({
            "thr": args.thr, "hold": args.hold, "filter": args.filter,
            "csv_glob": args.csv_glob, "data_root": args.data_root,
            "params_file": args.params,
            "ts": datetime.datetime.utcnow().isoformat()+"Z"
        }, f, ensure_ascii=False, indent=2)

    # params 스냅샷
    try:
        import shutil
        shutil.copyfile(args.params, os.path.join(args.outdir, "params_used.yml"))
    except Exception:
        pass

if __name__ == "__main__":
    main()
