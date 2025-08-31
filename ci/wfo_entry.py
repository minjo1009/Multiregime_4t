# ci/wfo_entry.py
import argparse, glob, os, json, importlib, importlib.util, yaml, pathlib, datetime, sys, re

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

def _import_from_path(py_path):
    spec = importlib.util.spec_from_file_location("wfo_dyn", py_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def _find_callable(module_or_path, fn_candidates):
    # Ensure repo paths are importable
    for p in [os.getcwd(), os.path.join(os.getcwd(), "src")]:
        if p not in sys.path:
            sys.path.insert(0, p)

    # 1) Try as module import
    if module_or_path:
        try:
            mod = importlib.import_module(module_or_path)
            for fn in fn_candidates:
                if fn and hasattr(mod, fn):
                    return getattr(mod, fn), f"module:{module_or_path}"
        except ModuleNotFoundError:
            pass

        # 2) Interpret module as path (backtest.runner -> backtest/runner.py)
        guess = module_or_path.replace(".", "/") + ".py"
        if os.path.isfile(guess):
            mod = _import_from_path(guess)
            for fn in fn_candidates:
                if fn and hasattr(mod, fn):
                    return getattr(mod, fn), f"path:{guess}"

    # 3) Direct script path
    if module_or_path and os.path.isfile(module_or_path):
        mod = _import_from_path(module_or_path)
        for fn in fn_candidates:
            if fn and hasattr(mod, fn):
                return getattr(mod, fn), f"script:{module_or_path}"

    # 4) Project-wide scan for a matching function
    for py in glob.glob("**/*.py", recursive=True):
        try:
            with open(py, "r", encoding="utf-8") as f: s = f.read()
        except Exception:
            continue
        if any(re.search(rf"\\bdef\\s+{fn}\\s*\\(", s) for fn in fn_candidates if fn):
            try:
                mod = _import_from_path(py)
                for fn in fn_candidates:
                    if fn and hasattr(mod, fn):
                        return getattr(mod, fn), f"scan:{py}"
            except Exception:
                continue

    # 5) Fallback common runners
    for py in ["run_4u.py", "run.py", "backtest/run_4u.py", "backtest/runner.py"]:
        if os.path.isfile(py):
            mod = _import_from_path(py)
            for fn in fn_candidates:
                if fn and hasattr(mod, fn):
                    return getattr(mod, fn), f"fallback:{py}"
    raise ImportError("Cannot locate entry callable via module/script/scan")

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

    # Accept module OR script
    module_or_path = entry.get("module") or entry.get("script") or "backtest.runner"
    fn_candidates = [entry.get("fn"), "run_once", "main", "run"]

    csvs = pick_csvs(args.data_root, args.csv_glob)
    ensure_dir(args.outdir)

    func, how = _find_callable(module_or_path, fn_candidates)
    print(f"[wfo_entry] using {how}")

    # Try full signature, then minimal
    run_kwargs = dict(
        csv_paths=csvs, params=cfg, outdir=args.outdir,
        thr=args.thr, hold=args.hold, filter_name=args.filter
    )
    try:
        func(**run_kwargs)
    except TypeError:
        func(csv_paths=csvs, params=cfg, outdir=args.outdir)

    write_minimum(args.outdir)
    with open(os.path.join(args.outdir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump({
            "thr": args.thr, "hold": args.hold, "filter": args.filter,
            "csv_glob": args.csv_glob, "data_root": args.data_root,
            "params_file": args.params, "entry_used": how,
            "ts": datetime.datetime.utcnow().isoformat()+"Z"
        }, f, ensure_ascii=False, indent=2)

    try:
        import shutil
        shutil.copyfile(args.params, os.path.join(args.outdir, "params_used.yml"))
    except Exception:
        pass

if __name__ == "__main__":
    main()
