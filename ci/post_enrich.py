# ci/post_enrich.py
# Enrich artifacts in outdir: compute win_rate, profit_factor, MCC (if y_true/y_pred exist)
import argparse, os, json, math
import pandas as pd
import numpy as np

PNL_CANDIDATES = ['pnl_close_based','pnl','pnl_value','pnl_usd','pnl_krw','pnl_pct','pnl_percent','ret','return','pnl_close']

def _read_json(p):
    try:
        with open(p,"r",encoding="utf-8") as f: return json.load(f)
    except Exception:
        return {}

def _read_csv(p):
    try:
        return pd.read_csv(p)
    except Exception:
        try:
            return pd.read_csv(p, engine="python", sep=",", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()

def _count_exits(trades: pd.DataFrame)->int:
    if trades is None or trades.empty: return 0
    ev = next((c for c in trades.columns if str(c).lower()=="event"), None)
    if ev:
        return int((trades[ev].astype(str).str.upper()=="EXIT").sum())
    # assume ENTRY/EXIT paired
    return int(len(trades)//2)

def _pnl_series(trades: pd.DataFrame):
    if trades is None or trades.empty: return None
    # prefer EXIT rows only
    df = trades.copy()
    ev = next((c for c in df.columns if str(c).lower()=="event"), None)
    if ev:
        df = df[df[ev].astype(str).str.upper()=="EXIT"]
    for c in PNL_CANDIDATES:
        if c in df.columns:
            s=pd.to_numeric(df[c], errors="coerce").fillna(0.0)
            if len(s): return s
    return None

def _winrate_profitfactor(trades: pd.DataFrame):
    s = _pnl_series(trades)
    if s is None or s.empty:
        return None, None, None
    n = int(len(s))
    pos=float(s[s>0].sum()); neg=float(s[s<0].sum())
    wr = float((s>0).sum())/max(1,n)
    pf = (pos/abs(neg)) if neg!=0 else float('nan')
    cp = float(s.sum())
    return wr, pf, cp

def _mcc(preds: pd.DataFrame):
    if preds is None or preds.empty: return None
    m = {str(c).lower():c for c in preds.columns}
    yt = next((m.get(k) for k in ['y_true','true','label','target'] if k in m), None)
    yp = next((m.get(k) for k in ['y_pred','pred','prediction','pred_label'] if k in m), None)
    if yt is None or yp is None:
        return None
    Yt = preds[yt].values
    Yp = preds[yp].values
    if preds[yp].dtype.kind in 'f':
        Yp = (Yp>=0.5).astype(int)
    tp=int(((Yt==1)&(Yp==1)).sum()); tn=int(((Yt==0)&(Yp==0)).sum())
    fp=int(((Yt==0)&(Yp==1)).sum()); fn=int(((Yt==1)&(Yp==0)).sum())
    den=(tp+fp)*(tp+fn)*(tn+fp)*(tn+fn)
    den=math.sqrt(den) if den else 0
    mcc=(tp*tn - fp*fn)/den if den else float('nan')
    return float(mcc)

def enrich(outdir: str):
    os.makedirs(outdir, exist_ok=True)
    trades_p = os.path.join(outdir,"trades.csv")
    preds_p  = os.path.join(outdir,"preds_test.csv")
    summ_p   = os.path.join(outdir,"summary.json")

    trades = _read_csv(trades_p) if os.path.exists(trades_p) else pd.DataFrame()
    preds  = _read_csv(preds_p) if os.path.exists(preds_p) else pd.DataFrame()
    summ   = _read_json(summ_p)

    # baseline fields
    n_exits = summ.get("exits")
    if n_exits is None:
        n_exits = _count_exits(trades)
        summ["exits"] = n_exits
    if "entries" not in summ:
        # estimate entries ≈ exits
        summ["entries"] = n_exits

    wr, pf, cp_from_trades = _winrate_profitfactor(trades)
    if wr is not None:
        summ["win_rate_from_trades"] = wr
        summ.setdefault("win_rate", wr)
    if pf is not None:
        summ["profit_factor_from_trades"] = pf
        summ.setdefault("profit_factor", pf)
    if cp_from_trades is not None:
        summ.setdefault("cum_pnl_close_based", cp_from_trades)

    mcc = _mcc(preds)
    if mcc is not None:
        summ["mcc"] = mcc

    # write back
    try:
        with open(summ_p,"w",encoding="utf-8") as f:
            json.dump(summ, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    enrich(args.outdir)
