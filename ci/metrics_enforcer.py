# ci/metrics_enforcer.py
# 목적: 러너 산출물(outdir)의 summary.json을 확정적으로 채움
#  - trades.csv에 PnL 컬럼이 없어도 데이터 CSV와 ENTRY/EXIT로 복원
#  - win_rate / profit_factor / cum_pnl_close_based / (가능시) mcc 계산
#  - 어떤 컬럼이 없어도 가능 범위 내에서 안전하게 동작

import os, json, argparse, glob
import pandas as pd
import numpy as np

def _find_datetime_col(cols):
    low = [c.lower() for c in cols]
    for cand in ("open_time", "timestamp", "time", "datetime", "date"):
        if cand in low: return cols[low.index(cand)]
    return None

def _find_close_col(cols):
    low = [c.lower() for c in cols]
    for cand in ("close", "close_price", "c"):
        if cand in low: return cols[low.index(cand)]
    return None

def _read_one_csv(data_root, pattern):
    matches = glob.glob(os.path.join(data_root, pattern), recursive=True)
    if not matches:
        raise FileNotFoundError(f"[metrics_enforcer] No CSV matched: {pattern}")
    df = pd.read_csv(matches[0])
    dcol = _find_datetime_col(df.columns)
    ccol = _find_close_col(df.columns)
    if not dcol or not ccol:
        raise ValueError(f"[metrics_enforcer] Data CSV missing datetime/close columns. have={list(df.columns)[:12]}")
    df[dcol] = pd.to_datetime(df[dcol])
    return df[[dcol, ccol]].rename(columns={dcol: "open_time", ccol: "close"})

def _detect_event_col(cols):
    low = [c.lower() for c in cols]
    return cols[low.index("event")] if "event" in low else None

def _pair_trades(tr):
    """ENTRY/EXIT 순서대로 1:1 페어링하여 trade_id를 만들어 반환"""
    evcol = _detect_event_col(tr.columns)
    if evcol is None:
        raise ValueError("[metrics_enforcer] trades.csv has no 'event' column")

    t = tr.copy()
    t["open_time"] = pd.to_datetime(t["open_time"], errors="coerce")
    t = t.sort_values("open_time", kind="stable").reset_index(drop=True)

    e = t[evcol].astype(str).str.upper()
    t["is_entry"] = e.str.contains("ENTRY")
    t["is_exit"]  = e.str.contains("EXIT")

    trade_id = []
    q = []  # FIFO queue of open trades
    next_id = 0
    for is_ent, is_ex in zip(t["is_entry"], t["is_exit"]):
        if is_ent:
            next_id += 1
            q.append(next_id)
            trade_id.append(next_id)
        elif is_ex and q:
            trade_id.append(q.pop(0))
        else:
            trade_id.append(np.nan)
    t["trade_id"] = trade_id

    ent = t[t["is_entry"]][["trade_id", "open_time"]].rename(columns={"open_time": "entry_time"})
    ex  = t[t["is_exit"] ][["trade_id", "open_time"]].rename(columns={"open_time": "exit_time"})
    pairs = ent.merge(ex, on="trade_id", how="inner")
    return pairs, evcol

def _infer_side(tr):
    # side/direction 컬럼 있으면 사용, 없으면 long(+1) 가정
    for c in tr.columns:
        cl = str(c).lower()
        if cl in ("side", "direction", "dir"):
            s = tr[c].astype(str).str.lower()
            return np.where(s.str.contains("short|sell|-1"), -1, 1)
    return None  # 못 찾으면 None 반환(후속에서 +1로 채움)

def enrich_metrics(outdir, data_root, csv_glob, thr=0.83, hold=9):
    summ_path = os.path.join(outdir, "summary.json")
    tr_path   = os.path.join(outdir, "trades.csv")
    pt_path   = os.path.join(outdir, "preds_test.csv")

    os.makedirs(outdir, exist_ok=True)
    summ = {}
    if os.path.exists(summ_path):
        try: summ = json.load(open(summ_path, "r", encoding="utf-8"))
        except Exception: summ = {}

    # --------- PnL/승률/프로핏팩터/누적PnL ---------
    if os.path.exists(tr_path):
        tr = pd.read_csv(tr_path)
        if "open_time" in map(str.lower, tr.columns):
            data = _read_one_csv(data_root, csv_glob)
            data["open_time"] = pd.to_datetime(data["open_time"])
            pairs, evcol = _pair_trades(tr)

            # 시세 조인 → entry/exit 가격
            ent = pairs[["trade_id", "entry_time"]].merge(
                data.rename(columns={"open_time": "entry_time", "close": "entry_price"}),
                on="entry_time", how="left"
            )
            ex  = pairs[["trade_id", "exit_time"]].merge(
                data.rename(columns={"open_time": "exit_time", "close": "exit_price"}),
                on="exit_time", how="left"
            )
            px  = ent.merge(ex, on="trade_id", how="inner")

            # side 추정(없으면 +1)
            side = _infer_side(tr)
            if side is None:
                px["side"] = 1
            else:
                # ENTRY 행의 side를 trade_id에 매핑
                t2 = tr.copy()
                t2["open_time"] = pd.to_datetime(t2["open_time"], errors="coerce")
                e = t2[evcol].astype(str).str.upper().str.contains("ENTRY")
                t2 = t2[e][["open_time"]].copy()
                t2["side"] = side[e].astype(int) if len(side)==len(tr) else 1
                # trade_id 재부여
                tmp_pairs, _ = _pair_trades(pd.DataFrame({
                    "open_time": tr["open_time"],
                    evcol: tr[evcol]
                }))
                sidemap = tmp_pairs.set_index("entry_time")["trade_id"].to_dict()
                t2["trade_id"] = t2["open_time"].map(sidemap)
                px = px.merge(t2[["trade_id","side"]], on="trade_id", how="left")
                px["side"] = px["side"].fillna(1).astype(int)

            # PnL
            px["pnl_close_based"] = (px["exit_price"] - px["entry_price"]) * px["side"]
            s = pd.to_numeric(px["pnl_close_based"], errors="coerce").fillna(0.0)

            # 요약치
            summ["exits"] = int(len(px))
            summ["win_rate"] = float((s > 0).sum()) / max(1, len(s))
            pos, neg = float(s[s > 0].sum()), float(s[s < 0].sum())
            summ["profit_factor"] = (pos / abs(neg)) if neg != 0 else None
            summ["cum_pnl_close_based"] = float(s.sum())

            # trades.csv에 EXIT 행만 PnL 채워넣기 (trade_id 없이 '순서'로 매핑)
            tr_enriched = tr.copy()
            # event 컬럼명
            evcol = _detect_event_col(tr_enriched.columns) or "event"
            exmask = tr_enriched[evcol].astype(str).str.upper().str.contains("EXIT", na=False)
            exit_idx = tr_enriched[exmask].index.to_list()
            k = min(len(exit_idx), len(px))
            if k > 0:
                tr_enriched.loc[exit_idx[:k], "pnl_close_based"] = px["pnl_close_based"].values[:k]
            tr_enriched.to_csv(tr_path, index=False)

        else:
            # trades.csv가 최소 키를 만족 못하면 exits만 보존
            summ["exits"] = int(summ.get("exits", 0))

    # ----------------- MCC (가능할 때만) -----------------
    if os.path.exists(pt_path):
        try:
            pt = pd.read_csv(pt_path)
            pcols = ["p","p_gate","gatep","prob","score","p_trend","p_range"]
            prob = next((c for c in pcols if c in pt.columns), None)
            if prob and _find_datetime_col(pt.columns):
                dcol = _find_datetime_col(pt.columns)
                pt[dcol] = pd.to_datetime(pt[dcol])
                data = _read_one_csv(data_root, csv_glob).sort_values("open_time").reset_index(drop=True)
                data["fwd"] = data["close"].shift(-int(hold)) / data["close"] - 1.0
                df = pt[[dcol, prob]].rename(columns={dcol:"open_time"}).merge(
                    data[["open_time","fwd"]], on="open_time", how="left"
                )
                y_true = (df["fwd"] > 0).astype(int)
                y_pred = (df[prob] >= float(thr)).astype(int)
                TP = int(((y_pred==1) & (y_true==1)).sum())
                TN = int(((y_pred==0) & (y_true==0)).sum())
                FP = int(((y_pred==1) & (y_true==0)).sum())
                FN = int(((y_pred==0) & (y_true==1)).sum())
                denom = np.sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN))
                mcc = ((TP*TN - FP*FN)/denom) if denom!=0 else 0.0
                summ["mcc"] = float(mcc)
                summ["cmatrix"] = {"TP":TP,"TN":TN,"FP":FP,"FN":FN}
        except Exception as e:
            # MCC는 선택사항이므로 실패해도 전체 실패로 만들지 않음
            summ.setdefault("mcc", None)

    json.dump(summ, open(summ_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
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