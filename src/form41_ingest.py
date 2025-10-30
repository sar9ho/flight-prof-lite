from pathlib import Path
import pandas as pd
import re

RAW  = Path("data_raw")
WORK = Path("data_work"); WORK.mkdir(exist_ok=True)

def _norm(df):
    df.columns = [re.sub(r"[^A-Z0-9]+", "_", c.upper()).strip("_") for c in df.columns]
    return df

def _first(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"Missing any of {candidates}. Have: {list(df.columns)[:30]}")

def build_p12a(p12a_csv: str):
    """Fuel dollars + gallons per month for WN from P-12(a)."""
    df = pd.read_csv(RAW / p12a_csv, low_memory=False)
    df = _norm(df)

    carrier = _first(df, ["CARRIER", "UNIQUECARRIER", "AIRLINEID"])
    year    = _first(df, ["YEAR"])
    month   = _first(df, ["MONTH"])

    gallon_candidates = [
        "STDOMGALLONS", "SDOMGALLONS",
        "SDOMT_GALLONS",
        "TDOMTGALLONS",
        "TSGALLONS",
        "TOTALGALLONS",
    ]
    cost_candidates = [
        "STDOMCOST", "SDOMCOST",
        "SDOMT_COST",
        "TDOMTCOST",
        "TSCOST",
        "TOTALCOST",
    ]

    gallons = next((c for c in gallon_candidates if c in df.columns), None)
    cost    = next((c for c in cost_candidates    if c in df.columns), None)
    if gallons is None or cost is None:
        raise KeyError(f"P-12(a) columns not found. Tried gallons={gallon_candidates}, cost={cost_candidates}")

    df[carrier] = df[carrier].astype(str).str.strip().str.upper()
    df = df[df[carrier] == "WN"].copy()

    g = (df.groupby([year, month], as_index=False)
           .agg({gallons: "sum", cost: "sum"}))

    g["month"] = pd.to_datetime(
        g[year].astype(int).astype(str) + "-" + g[month].astype(int).astype(str) + "-01"
    ).dt.strftime("%Y-%m")

    out = g.rename(columns={gallons: "fuel_gallons", cost: "fuel_expense"})
    return out[["month", "fuel_expense", "fuel_gallons"]]

def _match_any(name: str, patterns):
    return any(re.search(p, name) for p in patterns)

def build_p52(p52_csv: str):
    """Labor/Maint/Station per quarter for WN from P-5.2, then expand to months.
       Auto-detects reasonable columns if the exact ones aren’t present."""
    df = pd.read_csv(RAW / p52_csv, low_memory=False)
    df = _norm(df)

    carrier = _first(df, ["CARRIER", "UNIQUECARRIER", "AIRLINEID"])
    year    = _first(df, ["YEAR"])
    quarter = _first(df, ["QUARTER"])

    # Heuristics for buckets
    labor_patterns   = [r"PILOT", r"OTH(ER)?_?FLT", r"FLIGHT_PERSONNEL", r"BENEFIT", r"PERSONNEL",
                        r"WAGE", r"PAYROLL", r"SALAR"]
    maint_patterns   = [r"MAINT", r"REPAIR", r"OVERHAUL", r"AIRWORTH", r"MATERIALS"]
    station_patterns = [r"TOTAIROPEXPENSES", r"STATION", r"GROUND", r"HANDLING", r"TRAFFIC"]

    cols = df.columns.tolist()

    # Explicit favorites if present
    # Prefer explicit fields if present (underscore variants from your file)
    favorites_labor   = [c for c in ["PILOT_FLY_OPS","OTH_FLT_FLY_OPS","BENEFITS_FLY_OPS",
                                    "PILOTFLYOPS","OTHFLTFLYOPS","BENEFITSFLYOPS"] if c in cols]
    favorites_maint   = [c for c in ["TOT_DIR_MAINT","TOTDIRMAINT"] if c in cols]
    favorites_station = [c for c in ["TOT_AIR_OP_EXPENSES","TOTAIROPEXPENSES"] if c in cols]

    # Regex discovery
    if not favorites_labor:
        favorites_labor = [c for c in cols if _match_any(c, labor_patterns)]
    if not favorites_maint:
        favorites_maint = [c for c in cols if _match_any(c, maint_patterns)]
    if not favorites_station:
        favorites_station = [c for c in cols if _match_any(c, station_patterns)]

    # Require at least one column per bucket
    if not favorites_labor:
        raise KeyError("Could not find any labor-like columns in P-5.2. Check your field selections.")
    if not favorites_maint:
        raise KeyError("Could not find any maintenance-like columns in P-5.2. Check your field selections.")
    if not favorites_station:
        raise KeyError("Could not find any station/ops-like columns in P-5.2. Check your field selections.")

    numeric = list(set(favorites_labor + favorites_maint + favorites_station))
    # Detect "(000)" dollars: if max values look small, multiply by 1000.
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    scale = 1000.0 if df[numeric].max().max() < 1e9 else 1.0
    df[numeric] = df[numeric] * scale

    df[carrier] = df[carrier].astype(str).str.strip().str.upper()
    df = df[df[carrier] == "WN"].copy()

    agg = {c: "sum" for c in numeric}
    q = df.groupby([year, quarter], as_index=False).agg(agg)

    # For transparency, print which columns got used
    print("[P-5.2] Using columns:",
          "\n  labor   =", favorites_labor,
          "\n  maint   =", favorites_maint,
          "\n  station =", favorites_station)

    # Quarterly → monthly thirds
    rows = []
    for _, r in q.iterrows():
        y = int(r[year]); qtr = int(r[quarter])
        months = {1:[1,2,3], 2:[4,5,6], 3:[7,8,9], 4:[10,11,12]}[qtr]
        labor_sum   = float(sum(r[c] for c in favorites_labor))
        maint_sum   = float(sum(r[c] for c in favorites_maint))
        station_sum = float(sum(r[c] for c in favorites_station))
        for m in months:
            rows.append({
                "month": f"{y}-{m:02d}",
                "labor_expense":   labor_sum   / 3.0,
                "maint_expense":   maint_sum   / 3.0,
                "station_other":   station_sum / 3.0,
            })
    out = pd.DataFrame(rows)
    return out[["month","labor_expense","maint_expense","station_other"]]

def build_fact_financials(p12a_csv: str, p52_csv: str):
    fuel  = build_p12a(p12a_csv)
    ops   = build_p52(p52_csv)

    seg = pd.read_csv(WORK / "fact_segments.csv")
    months = seg["month"].unique()

    fin = pd.merge(ops, fuel, on="month", how="outer").fillna(0.0)
    fin = fin[fin["month"].isin(months)].copy()

    fin = fin[["month","fuel_expense","labor_expense","maint_expense","station_other","fuel_gallons"]]
    fin.to_csv(WORK / "fact_financials.csv", index=False)
    print(f"Wrote {WORK/'fact_financials.csv'} with {len(fin)} months.")

if __name__ == "__main__":
    build_fact_financials("FORM41_P12A_2023.csv", "FORM41_P52_2023.csv")
