# src/seed_mock_data.py
from pathlib import Path
import pandas as pd

DATA_WORK = Path("data_work")
DATA_WORK.mkdir(exist_ok=True)

# ---------- fact_segments.csv ----------
# minimal columns expected by allocation.py
fact_segments = pd.DataFrame([
    # month, origin, dest, fleet_type, departures, block_hours, ASMs, RPMs, pax
    ["2023-07", "DAL", "HOU", "B737-700", 300,  240.0,  90_000_000, 72_000_000, 120_000],
    ["2023-07", "DAL", "AUS", "B737-700", 200,  140.0,  40_000_000, 30_000_000,  55_000],
    ["2023-07", "DAL", "DEN", "B737-800", 150,  210.0, 120_000_000, 87_000_000, 140_000],
    ["2023-07", "HOU", "MCO", "B737-8",   120,  216.0, 110_000_000, 75_000_000, 118_000],
    ["2023-07", "PHX", "LAS", "B737-700", 250,  120.0,  35_000_000, 28_000_000,  46_000],
])
fact_segments.columns = ["month","origin","dest","fleet_type","departures",
                         "block_hours","ASMs","RPMs","pax"]

# ---------- fact_financials.csv ----------
# monthly airline-level totals (toy numbers)
fact_financials = pd.DataFrame([
    # month, fuel_expense, labor_expense, maint_expense, station_other, fuel_gallons
    ["2023-07", 125_000_000, 220_000_000, 85_000_000, 95_000_000, 75_000_000],
])
fact_financials.columns = ["month","fuel_expense","labor_expense",
                           "maint_expense","station_other","fuel_gallons"]

# ---------- fact_fares.csv ----------
# yield_est is $/RPM (toy)
fact_fares = pd.DataFrame([
    # month, origin, dest, yield_est, avg_fare, pax
    ["2023-07","DAL","HOU", 0.145, 95, 120_000],
    ["2023-07","DAL","AUS", 0.155, 88,  55_000],
    ["2023-07","DAL","DEN", 0.135,120, 140_000],
    ["2023-07","HOU","MCO", 0.125,130, 118_000],
    ["2023-07","PHX","LAS", 0.160, 76,  46_000],
])
fact_fares.columns = ["month","origin","dest","yield_est","avg_fare","pax"]

# write files
fact_segments.to_csv(DATA_WORK/"fact_segments.csv", index=False)
fact_financials.to_csv(DATA_WORK/"fact_financials.csv", index=False)
fact_fares.to_csv(DATA_WORK/"fact_fares.csv", index=False)

print("Seeded mock CSVs in data_work/:")
for f in ["fact_segments.csv","fact_financials.csv","fact_fares.csv"]:
    print(" -", DATA_WORK/f)
