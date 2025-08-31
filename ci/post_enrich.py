# ci/post_enrich.py (kept for reference; embedded version is run automatically)
import argparse, os
from wfo_entry import post_enrich as _run  # reuse embedded
if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--outdir", required=True)
    args=ap.parse_args(); _run(args.outdir)
