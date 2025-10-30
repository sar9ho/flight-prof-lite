"""
Flight-Prof Lite: Allocation Engine (v1)
- Reads cleaned fact tables from data_work/
- Applies allocation weights from allocation_config.yaml
- Writes fact_route_economics.csv to data_work/
"""
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

CONFIG = Path("allocation_config.yaml")
DATA_WORK = Path("data_work")

def load_config():
    with open(CONFIG, "r") as f:
        return yaml.safe_load(f)

def load_inputs():
    # expected columns:
    # seg:  month, origin, dest, fleet_type, departures, block_hours, ASMs, RPMs, pax
    # fin:  month, fuel_expense, labor_expense, maint_expense, station_other, fuel_gallons
    # fare: month, origin, dest, yield_est, avg_fare, pax
    seg = pd.read_csv(DATA_WORK / "fact_segments.csv")
    fin = pd.read_csv(DATA_WORK / "fact_financials.csv")
    fares = pd.read_csv(DATA_WORK / "fact_fares.csv")
    return seg, fin, fares

def allocate(month: str):
    cfg = load_config()
    seg, fin, fares = load_inputs()

    # --- filter this month ---
    segm = seg[seg["month"] == month].copy()
    finm = fin[fin["month"] == month].iloc[0]
    faresm = fares[fares["month"] == month].copy()

    # --- merge fares by month+OD (IMPORTANT) ---
    segm = segm.merge(
        faresm[["month", "origin", "dest", "yield_est", "avg_fare", "pax"]],
        on=["month", "origin", "dest"],
        how="left",
        suffixes=("", "_fare"),
    )

    # --- revenue & RASM ---
    # fallback yield of 12.5¢/RPM if missing
    segm["yield_est"] = segm["yield_est"].fillna(0.125)
    segm["RPMs"] = segm["RPMs"].fillna(0.0)
    segm["ASMs"] = segm["ASMs"].fillna(0.0)

    segm["revenue"] = segm["RPMs"] * segm["yield_est"]
    segm["rasm"] = np.where(segm["ASMs"] > 0, segm["revenue"] / segm["ASMs"], np.nan)

    # --- fuel allocation: block_hours * burn_rate per fleet ---
    burn = cfg["fuel"]["burn_rate_hr"]  # e.g., {"B737-700": 850, ...}
    default_burn = min(burn.values()) if len(burn) else 1.0
    segm["burn_rate_hr"] = segm["fleet_type"].map(burn).fillna(default_burn)
    segm["block_hours"] = segm["block_hours"].fillna(0.0)

    segm["fuel_driver"] = segm["block_hours"] * segm["burn_rate_hr"]
    fuel_driver_sum = segm["fuel_driver"].sum()
    segm["fuel_share"] = np.where(fuel_driver_sum > 0, segm["fuel_driver"] / fuel_driver_sum, 0.0)
    segm["fuel_cost"] = segm["fuel_share"] * float(finm["fuel_expense"])

    # --- labor allocation: 0.7 block_hours + 0.3 departures ---
    lw = cfg["labor"]["weights"]  # {"block_hours": 0.7, "departures": 0.3}
    segm["departures"] = segm["departures"].fillna(0.0)
    bh_sum = segm["block_hours"].sum()
    dp_sum = segm["departures"].sum()
    bh_share = np.where(bh_sum > 0, segm["block_hours"] / bh_sum, 0.0)
    dep_share = np.where(dp_sum > 0, segm["departures"] / dp_sum, 0.0)

    segm["labor_share"] = lw.get("block_hours", 0.0) * bh_share + lw.get("departures", 0.0) * dep_share
    segm["labor_cost"] = segm["labor_share"] * float(finm["labor_expense"])

    # --- maintenance allocation: (default) block_hours (optionally + departures) ---
    mw = cfg["maintenance"]["weights"]  # e.g., {"block_hours": 1.0}
    segm["maint_share"] = mw.get("block_hours", 0.0) * bh_share + mw.get("departures", 0.0) * dep_share
    segm["maint_cost"] = segm["maint_share"] * float(finm["maint_expense"])

    # --- station/other: 0.5 departures + 0.5 pax ---
    sw = cfg["station_other"]["weights"]  # {"departures": 0.5, "pax": 0.5}
    segm["pax"] = segm["pax"].fillna(0.0)
    pax_sum = segm["pax"].sum()
    pax_share = np.where(pax_sum > 0, segm["pax"] / pax_sum, 0.0)

    segm["station_share"] = sw.get("departures", 0.0) * dep_share + sw.get("pax", 0.0) * pax_share
    segm["station_cost"] = segm["station_share"] * float(finm["station_other"])

    # --- totals & KPIs ---
    segm["total_cost"] = segm[["fuel_cost", "labor_cost", "maint_cost", "station_cost"]].sum(axis=1)
    segm["casm"] = np.where(segm["ASMs"] > 0, segm["total_cost"] / segm["ASMs"], np.nan)
    segm["margin"] = segm["revenue"] - segm["total_cost"]
    segm["margin_per_ASM"] = np.where(segm["ASMs"] > 0, segm["margin"] / segm["ASMs"], np.nan)

    out_cols = [
        "month", "origin", "dest", "fleet_type", "departures", "block_hours",
        "ASMs", "RPMs", "pax",
        "revenue", "rasm",
        "fuel_cost", "labor_cost", "maint_cost", "station_cost",
        "total_cost", "casm", "margin", "margin_per_ASM"
    ]
    DATA_WORK.mkdir(exist_ok=True)
    segm[out_cols].to_csv(DATA_WORK / "fact_route_economics.csv", index=False)
    print(f"Wrote {DATA_WORK/'fact_route_economics.csv'} with {len(segm)} rows for {month}")

if __name__ == "__main__":
    import argparse
    import shutil

    p = argparse.ArgumentParser()
    p.add_argument("--month", required=True, help="YYYY-MM")
    p.add_argument("--use-shocked", action="store_true",
                   help="Use data_work/fact_financials_shocked.csv if present")
    args = p.parse_args()

    # Optionally swap in shocked financials for this run (non-destructive: copy-over)
    if args.use_shocked and (DATA_WORK / "fact_financials_shocked.csv").exists():
        shutil.copyfile(DATA_WORK / "fact_financials_shocked.csv", DATA_WORK / "fact_financials.csv")
        print("Using shocked financials for allocation…")

    allocate(args.month)
