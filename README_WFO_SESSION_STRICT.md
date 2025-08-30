# WFO_Session_Strict Package

Contents:
- `.github/workflows/wfo_session_strict.yml` — STRICT GitHub Actions workflow (pinned SHAs, ASCII-only, no heredoc).
- `scripts/preflight_strict.py` — Same preflight used by the workflow (optional to keep in repo).
- This package assumes your **code zip** exposes an entrypoint at one of:
  `run_4u.py` or `backtest/run_4u.py` or `run.py` or `backtest/runner.py`.
- Data zip must contain a CSV with columns: `open_time,open,high,low,close,volume`.

How to use:
1) Add these files to your repo (keep the same paths).
2) Open GitHub → Actions → run `WFO_Session_Strict`.
3) Provide `CODE_ZIP_URL`, `DATA_ZIP_URL`, and (optional) adjust inputs like `CSV_GLOB`, `FEES_BPS`, `SPLITS`, `TZ`, `CHAMPION_CONFIG`.
4) Artifacts produced: `single_results.zip`, `wfo_results.zip`, `bundle_results.zip`.

Notes:
- Full 40-char SHA pinning is validated for `setup-python` and `upload-artifact`.
- The workflow writes `conf/config.effective.yml` automatically from preflight.
- Session windows (local time `TZ`): ASIA=09–17, EU=17–01, US=01–09.
