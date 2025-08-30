# WFO + Session Package (Strict)

### Repo layout matched
Your screenshots show these files at the repo root:
- `strategy_v2_codepack_v2.1.3.zip`
- `ETHUSDT_1min_2020_2025.zip`
- `.github/workflows/` already exists

This package adds:
- `.github/workflows/wfo_session_strict.yml`
- `scripts/preflight_strict.py`
- `inputs_example.json`

### How to run
1. Commit these files as-is.
2. In GitHub → Actions → `WFO_Session_Strict` → Run workflow.
3. Inputs example (replace `<OWNER>/<REPO>` accordingly):
   - CODE_ZIP_URL: https://raw.githubusercontent.com/<OWNER>/<REPO>/main/strategy_v2_codepack_v2.1.3.zip
   - DATA_ZIP_URL: https://raw.githubusercontent.com/<OWNER>/<REPO>/main/ETHUSDT_1min_2020_2025.zip
   - CSV_GLOB: **/*ETHUSDT*1min*2020*2025*.csv
   - FEES_BPS: 7.5
   - CHAMPION_CONFIG: grid_p0.83_tp2.2_sl0.45_cd34_mh12_ofi0.42_thi0.74
   - SPLITS: 4
   - TZ: Asia/Seoul

### What it does
- Single-run reproduction of champion (creates summary.json, gating_debug.json, preds_test.csv, trades.csv, summary_cost.json if available).
- WFO splits (K parts).
- Session split (ASIA/EU/US).
- Uploads: `single_results.zip`, `wfo_results.zip`, `bundle_results.zip`.

### Policy guards (Strict)
- Full SHA pinning for `actions/setup-python` and `actions/upload-artifact`.
- ASCII-only YAML; no heredoc; safe echo for python generation.
- Preflight: CSV detection + required columns; entrypoint auto-detect; artifacts guaranteed.

If your entrypoint differs, ensure one of these exists inside the code zip:
`run_4u.py` or `backtest/run_4u.py` or `run.py` or `backtest/runner.py`.
