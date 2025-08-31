# ci/patch_runner_append.py
"""
Appends a post-processing block to backtest/runner.py to ensure:
- trades.csv contains pnl_close_based (if missing, fallback to 'pnl' or computed series placeholder)
- summary.json includes win_rate, profit_factor, cum_pnl_close_based (if pnl exists)
Usage:
  python ci/patch_runner_append.py --repo-root .
"""
import argparse, os, io, re, sys

TEMPLATE = r