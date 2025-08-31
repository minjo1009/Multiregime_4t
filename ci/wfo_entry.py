# ci/wfo_entry.py  (hardened autodetect v3)
import argparse, glob, os, json, importlib, importlib.util, yaml, pathlib, datetime, sys, re, inspect

EXCLUDE_DIR_HINTS = ["ci/", "/ci/", "/.github/", "/tests/", "/test/"]
EXCLUDE_FILE_HINTS = ["preflight", "pre_flight", "check", "lint", "setup"]

PREFERRED_PATH_HINTS = ["backtest", "strategy", "runner", "train", "run_4u", "run"]

DESIRED_ARGS = ["csv_paths","params","outdir","thr","hold","filter_name"]

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

def _should_exclude_path(path):
    low = path.replace("\\\\","/").lower()
    if any(h in low for h in EXCLUDE_DIR_HINTS): return True
    base = os.path.basename(low)
    if any(h in base for h in EXCLUDE_FILE_HINTS): return True
    return False

def _path_pref_score(path):
    low = path.replace("\\\\","/").lower()
    score = 0
    for i,kw in enumerate(PREFERRED_PATH_HINTS):
        if kw in low: score += (10 - i)  # earlier hints worth more
    return score

def _score_callable(func, source_path):
    try:
        sig = inspect.signature(func)
        params = set(sig.parameters.keys())
        score = sum(1 for k in DESIRED_ARGS if k in params)
        # prefer functions not requiring *args/**kwargs only
        if any(p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD) for p in sig.parameters.values()):
            score -= 1
    except Exception:
        score = 0
    # path preference
    score += _path_pref_score(source_path or "")
    return score

def _call_compatible(func, **kwargs):
    try:
        sig = inspect.signature(func)
        accepted = {k:v for k,v in kwargs.items() if k in sig.parameters}
        return func(**accepted)
    except Exception as e:
        # last resort: try no-arg call (for scripts exposing main() without params)
        try:
            return func()
        except Exception as e2:
            raise e2

def _find_callable(entry_module, entry_script, fn_candidates):
    # Ensure repo paths are importable
    for p in [os.getcwd(), os.path.join(os.getcwd(), "src")]:
        if p not in sys.path: sys.path.insert(0, p)

    candidates = []  # list of (callable, how, source_path, score)

    def consider_module(mod_name, how_hint):
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            return
        for fn in fn_candidates:
            if fn and hasattr(mod, fn):
                obj = getattr(mod, fn)
                src = getattr(obj, "__code__", None)
                srcp = getattr(src, "co_filename", mod.__file__ if hasattr(mod,'__file__') else "")
                if srcp and _should_exclude_path(srcp): continue
                sc = _score_callable(obj, srcp or "")
                candidates.append((obj, f"{how_hint}:{mod_name}", srcp or "", sc))

    def consider_path(py_path, how_hint):
        if not os.path.isfile(py_path): return
        if _should_exclude_path(py_path): return
        try:
            mod = _import_from_path(py_path)
        except Exception:
            return
        for fn in fn_candidates:
            if fn and hasattr(mod, fn):
                obj = getattr(mod, fn)
                sc = _score_callable(obj, py_path)
                candidates.append((obj, f"{how_hint}:{py_path}", py_path, sc))

    # 1) Explicit overrides first
    if entry_module: consider_module(entry_module, "module")
    if entry_script: consider_path(entry_script, "script")

    # 2) If module provided but import failed, try path guess
    if entry_module:
        guess = entry_module.replace(".", "/") + ".py"
        consider_path(guess, "path")

    # 3) Project-wide scan
    for py in glob.glob("**/*.py", recursive=True):
        if _should_exclude_path(py): continue
        try:
            with open(py, "r", encoding="utf-8") as f: s = f.read()
        except Exception:
            continue
        for fn in [x for x in fn_candidates if x]:
            pattern = r"\bdef\s+" + re.escape(fn) + r"\s*\("
            try:
                if re.search(pattern, s):
                    consider_path(py, "scan")
            except re.error:
                continue

    # 4) Fallback common runners
    for py in ["run_4u.py", "run.py", "backtest/run_4u.py", "backtest/runner.py"]:
        consider_path(py, "fallback")

    if not candidates:
        raise ImportError("Cannot locate entry callable via module/script/scan")

    # Pick best by score
    candidates.sort(key=lambda x: x[3], reverse=True)
    return candidates[0][0], candidates[0][1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", "--config", dest="params", required=True)
    ap.add_argument("--data-root", default=".")
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--thr", type=float)
    ap.add_argument("--hold", type=int)
    ap.add_argument("--filter", type=str)
    ap.add_argument("--outdir", required=True)
    # explicit overrides (optional)
    ap.add_argument("--entry-module")
    ap.add_argument("--entry-script")
    ap.add_argument("--entry-fn")
    args = ap.parse_args()

    cfg = load_params(args.params)
    entry = (cfg.get("entry") or {})

    # Accept module OR script, allow CLI override
    entry_module = args.entry_module or entry.get("module")
    entry_script = args.entry_script or entry.get("script")
    fn_candidates = [args.entry_fn or entry.get("fn"), "run_once", "main", "run"]

    csvs = pick_csvs(args.data_root, args.csv_glob)
    ensure_dir(args.outdir)

    func, how = _find_callable(entry_module, entry_script, fn_candidates)
    print(f"[wfo_entry] using {how}")

    run_kwargs = dict(
        csv_paths=csvs, params=cfg, outdir=args.outdir,
        thr=args.thr, hold=args.hold, filter_name=args.filter
    )
    _call_compatible(func, **run_kwargs)

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
