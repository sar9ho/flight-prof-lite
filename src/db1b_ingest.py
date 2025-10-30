from pathlib import Path
import pandas as pd
import numpy as np
import re

RAW  = Path("data_raw")
WORK = Path("data_work"); WORK.mkdir(exist_ok=True)

def _norm(df):
    df.columns = [re.sub(r"[^A-Z0-9]+", "_", c.upper()).strip("_") for c in df.columns]
    return df

def _first(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    raise KeyError(f"Missing any of {candidates}. Have: {list(df.columns)[:25]}")

def _q_to_months(q):
    return {1:[1,2,3], 2:[4,5,6], 3:[7,8,9], 4:[10,11,12]}[int(q)]

def build_fact_fares(db1b_csv="DB1B_MARKET_2023.csv", carrier="WN"):
    # Load DB1B + normalize
    df = pd.read_csv(RAW/db1b_csv, low_memory=False)
    df = _norm(df)

    year     = _first(df, ["YEAR"])
    quarter  = _first(df, ["QUARTER"])
    carrierc = _first(df, ["REPORTING_CARRIER","RPCARRIER","CARRIER","UNIQUECARRIER","AIRLINE_ID"])
    origin   = _first(df, ["ORIGIN"])
    dest     = _first(df, ["DEST"])

    # prefer total market fare (quarterly), else avg fare * pax
    fare_total_col = next((c for c in ["MARKET_FARE","MKTFARE"] if c in df.columns), None)
    pax_col        = _first(df, ["PASSENGERS","PAX"])
    avg_fare_col   = next((c for c in ["AVERAGE_FARE","AVG_FARE","FARE"] if c in df.columns), None)

    # filter for Southwest
    df[carrierc] = df[carrierc].astype(str).str.strip().str.upper()
    df = df[df[carrierc] == carrier].copy()

    # numeric clean
    for c in [pax_col, fare_total_col, avg_fare_col]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # build monthly pax shares from T-100 (by quarter, OD, month)
    seg = pd.read_csv(WORK/"fact_segments.csv", dtype={"origin":str,"dest":str})
    seg["year"]  = pd.to_datetime(seg["month"]).dt.year
    seg["mnum"]  = pd.to_datetime(seg["month"]).dt.month
    seg["qtr"]   = ((seg["mnum"] - 1) // 3 + 1).astype(int)

    # monthly pax per OD
    odm = (seg.groupby(["year","qtr","origin","dest","mnum"], as_index=False)
              .agg(pax_m=("pax","sum")))

    # quarter totals per OD
    odq = (odm.groupby(["year","qtr","origin","dest"], as_index=False)
              .agg(pax_q_total=("pax_m","sum")))

    # join & compute share; guard zero-quarters
    w = odm.merge(odq, on=["year","qtr","origin","dest"], how="left")
    w["share"] = np.where(w["pax_q_total"] > 0, w["pax_m"] / w["pax_q_total"], np.nan)

    # dict for fast lookup: (year, qtr, mnum, origin, dest) -> share
    share_map = {(int(r.year), int(r.qtr), int(r.mnum), str(r.origin)[:3], str(r.dest)[:3]): float(r.share)
                 for r in w.itertuples(index=False)}

    # expand DB1B quarter to months using T-100 shares (fallback 1/3)
    rows = []
    for _, r in df.iterrows():
        y     = int(r[year])
        q     = int(r[quarter])
        o     = str(r[origin])[:3]
        d     = str(r[dest])[:3]
        pax_q = float(r[pax_col])
        rev_q = float(r[fare_total_col]) if fare_total_col else float(r.get(avg_fare_col, 0.0)) * pax_q

        months = _q_to_months(q)
        fallback_share = 1.0/3.0
        for m in months:
            sh = share_map.get((y, q, m, o, d), np.nan)
            if not np.isfinite(sh):
                sh = fallback_share
            rows.append({
                "month":  f"{y:04d}-{m:02d}",
                "origin": o,
                "dest":   d,
                "pax":    pax_q * sh,   # monthly pax via T-100 share
                "rev":    rev_q * sh,   # monthly revenue via T-100 share
            })

    fares_m = pd.DataFrame(rows)

    # aggregate to unique month+OD (DB1B has many samples per market)
    fares_m = (fares_m.groupby(["month","origin","dest"], as_index=False)
                        .agg(pax=("pax","sum"), rev=("rev","sum")))

    # join T-100 RPMs; compute yield & avg fare
    seg_mkt = (seg.groupby(["month","origin","dest"], as_index=False)
                 .agg(RPMs=("RPMs","sum")))

    fares_m = fares_m.merge(seg_mkt, on=["month","origin","dest"], how="left")

    # yield_est = revenue / RPMs (fallback to 12.5¢ if RPMs missing/zero)
    fares_m["yield_est"] = np.where(fares_m["RPMs"] > 0, fares_m["rev"] / fares_m["RPMs"], 0.125)

    # avg_fare = revenue / pax
    fares_m["avg_fare"] = np.where(fares_m["pax"] > 0, fares_m["rev"] / fares_m["pax"], np.nan)

    # coverage/confidence
    seg_coverage = (seg.groupby(["month","origin","dest"], as_index=False)
                    .agg(pax_t100=("pax","sum")))
    fares_m = fares_m.merge(seg_coverage, on=["month","origin","dest"], how="left")

    # missing/zero T-100 pax treated as 0 (aka low coverage)
    pax_t100 = fares_m["pax_t100"].fillna(0.0)
    fares_m["coverage"] = np.where(pax_t100 > 0, fares_m["pax"]/pax_t100, 0.0)

    fares_m["confidence"] = pd.cut(
        fares_m["coverage"].clip(0, 1.01),
        bins=[-0.01, 0.25, 0.6, 1.01],
        labels=["low", "medium", "high"]
    )


    out = fares_m[["month","origin","dest","yield_est","avg_fare","pax","coverage","confidence"]]
    out.to_csv(WORK/"fact_fares.csv", index=False)
    print(f"Wrote {WORK/'fact_fares.csv'} with {len(out)} rows (unique month+OD).")

if __name__ == "__main__":
    build_fact_fares()
