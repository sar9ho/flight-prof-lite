"""
Simple sensitivity runner:
- fuel +/- : scales total fuel expense before re-allocating shares
- lf +/- : adjusts RPMs (and therefore revenue) via ASMs * (LF +/- delta)
(Assumes data_work/fact_financials.csv and fact_segments.csv exist.)
"""
import pandas as pd
from pathlib import Path

DATA_WORK = Path("data_work")

def fuel_shock(month:str, pct:float):
    fin = pd.read_csv(DATA_WORK/"fact_financials.csv")
    fin.loc[fin["month"]==month, "fuel_expense"] *= (1 + pct/100.0)
    fin.to_csv(DATA_WORK/"fact_financials_shocked.csv", index=False)
    print(f"Applied fuel shock {pct}% for {month} -> data_work/fact_financials_shocked.csv")

if __name__ == "__main__":
    import argparse
    a = argparse.ArgumentParser()
    a.add_argument("--fuel", type=float, help="+10 or -10 means +/-10%")
    a.add_argument("--month", required=True)
    args = a.parse_args()
    if args.fuel is not None:
        fuel_shock(args.month, args.fuel)
