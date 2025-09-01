# ci/metrics_enforcer.py
import os, json, argparse, glob
import pandas as pd
import numpy as np

def _read_one_csv(data_root, pattern):
    matches = glob.glob(os.path.join(data_root, pattern), recursive=True)
    if not matches:
        raise FileNotFoundError(f"No CSV matched: {pattern}")
    # 첫 번째만 사용 (여러개면 동일 심볼 가정)
    df = pd.read_csv(matches[0])
    # 표준화
    for c in df.columns:
        if str(c).lower() in ("open_time","timestamp","time","datetime","date"):
            dtcol = c; break
    else:
        raise ValueError("No datetime-like column in data CSV")
    df[dtcol] = pd.to_datetime(df[dtcol])
    # close 컬럼 탐색
    for c in df.columns:
        if str(c).lower() in ("close","close_price","c"): closecol = c; break
    else:
        raise ValueError("No close column in data CSV")
    return df[[dtcol, closecol]].rename(columns={dtcol:"open_time", closecol:"close"})

def _pair_trades(trades):
    """ENTRY/EXIT 순서대로 1:1 페어링, trade_id 부여"""
    t = trades.copy()
    t["open_time"] = pd.to_datetime(t["open_time"])
    t = t.sort_values("open_time").reset_index(drop=True)
    evcol = next((c for c in t.columns if str(c).lower()=="event"), None)
    if evcol is None: raise ValueError("trades.csv has no 'event' column")
    t["is_entry"] = t[evcol].astype(str).str.upper().str.contains("ENTRY")
    t["is_exit"]  = t[evcol].astype(str).str.upper().str.contains("EXIT")
    # 누적 엔트리 id를 만들어 EXIT가 동일 id를 갖도록 누적-출력
    tid = 0; open_ids=[]
    stack=[]
    for ent, ex in zip(t["is_entry"], t["is_exit"]):
        if ent:
            tid += 1; stack.append(tid); open_ids.append(tid)
        elif ex and stack:
            open_ids.append(stack.pop(0))
        else:
            open_ids.append(np.nan)
    t["trade_id"] = open_ids
    t = t.dropna(subset=["trade_id"]).copy()
    return t

def _infer_side(trades):
    # side/direction 컬럼 있으면 사용, 없으면 long 가정
    for c in trades.columns:
        cl = str(c).lower()
        if cl in ("side","direction","dir"):
            s = trades[c].astype(str).str.lower()
            return np.where(s.str.contains("short|sell|-1"), -1, 1)
    return np.ones(len(trades), dtype=int)

def enrich_metrics(outdir, data_root, csv_glob, thr=0.83, hold=9):
    summ_path = os.path.join(outdir, "summary.json")
    tr_path   = os.path.join(outdir, "trades.csv")
    pt_path   = os.path.join(outdir, "preds_test.csv")

    summ = {}
    if os.path.exists(summ_path):
        summ = json.load(open(summ_path, "r", encoding="utf-8"))

    if os.path.exists(tr_path):
        tr = pd.read_csv(tr_path)
        # 필요한 최소 컬럼 체크
        need = {"open_time"}
        if not need.issubset(set(map(str.lower, tr.columns))):
            # 포기: 최소 키도 없으면 exits만 유지
            summ["exits"] = int(summ.get("exits", 0))
        else:
            data = _read_one_csv(data_root, csv_glob)
            # 조인 준비
            tr["open_time"] = pd.to_datetime(tr["open_time"])
            data["open_time"] = pd.to_datetime(data["open_time"])
            # ENTRY/EXIT 페어링
            t = _pair_trades(tr)
            # 각 행 시세 조인
            t = t.merge(data, on="open_time", how="left")
            t = t.rename(columns={"close":"price"})
            # trade_id별 entry/exit 분리
            side = _infer_side(t)
            t["side_inferred"] = side
            ent = t[t["is_entry"]][["trade_id","price","open_time"]].rename(columns={"price":"entry_price","open_time":"entry_time"})
            ex  = t[t["is_exit"]][["trade_id","price","open_time"]].rename(columns={"price":"exit_price","open_time":"exit_time"})
            pairs = ent.merge(ex, on="trade_id", how="inner")
            pairs["side"] = 1  # 기본 long
            # 만약 entry/exit에 side가 살아있다면 보정
            if "side_inferred" in t.columns:
                # 첫 이벤트의 side를 사용
                s_map = t[t["is_entry"]].set_index("trade_id")["side_inferred"]
                pairs["side"] = pairs["trade_id"].map(s_map).fillna(1).astype(int)

            # PnL (close 기반)
            pairs["pnl_close_based"] = (pairs["exit_price"] - pairs["entry_price"]) * pairs["side"]
            # 요약치
            s = pairs["pnl_close_based"].fillna(0.0)
            summ["exits"] = int(len(pairs))
            summ["win_rate"] = float((s>0).sum()) / max(1,len(s))
            pos, neg = float(s[s>0].sum()), float(s[s<0].sum())
            summ["profit_factor"] = (pos/abs(neg)) if neg!=0 else None
            summ["cum_pnl_close_based"] = float(s.sum())

            # trades.csv에 pnl 컬럼 보강 저장(있어도 유지)
            tr_out = tr.merge(pairs[["trade_id","entry_time","exit_time","pnl_close_based"]], on="trade_id", how="left")
            tr_out.to_csv(tr_path, index=False)

    # MCC (가능할 때만)
    if os.path.exists(pt_path):
        pt = pd.read_csv(pt_path)
        # 확률/점수 후보
        pcol = next((c for c in ["p","p_gate","gatep","prob","score","p_trend","p_range"] if c in pt.columns), None)
        if pcol and "open_time" in pt.columns:
            data = _read_one_csv(data_root, csv_glob)
            pt["open_time"] = pd.to_datetime(pt["open_time"])
            data["open_time"] = pd.to_datetime(data["open_time"])
            # hold-step fwd return
            data = data.sort_values("open_time").reset_index(drop=True)
            data["fwd"] = data["close"].shift(-int(hold)) / data["close"] - 1.0
            df = pt.merge(data[["open_time","fwd"]], on="open_time", how="left")
            y_true = (df["fwd"] > 0).astype(int)
            y_pred = (df[pcol] >= float(thr)).astype(int)
            TP = int(((y_pred==1) & (y_true==1)).sum())
            TN = int(((y_pred==0) & (y_true==0)).sum())
            FP = int(((y_pred==1) & (y_true==0)).sum())
            FN = int(((y_pred==0) & (y_true==1)).sum())
            denom = np.sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN))
            mcc = ((TP*TN - FP*FN)/denom) if denom!=0 else 0.0
            summ["mcc"] = float(mcc)
            summ["cmatrix"] = {"TP":TP,"TN":TN,"FP":FP,"FN":FN}

    os.makedirs(outdir, exist_ok=True)
    json.dump(summ, open(summ_path,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    print("[metrics_enforcer] summary updated:", summ)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--csv-glob", required=True)
    ap.add_argument("--thr", type=float, default=0.83)
    ap.add_argument("--hold", type=int, default=9)
    args = ap.parse_args()
    enrich_metrics(args.outdir, args.data_root, args.csv_glob, args.thr, args.hold)