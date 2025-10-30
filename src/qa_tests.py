import pandas as pd
from pathlib import Path

DATA_WORK = Path("data_work")

def run_basic_checks():
    seg = pd.read_csv(DATA_WORK/"fact_segments.csv")
    assert (seg["ASMs"] >= seg["RPMs"]).all(), "RPMs cannot exceed ASMs"
    assert seg["departures"].ge(0).all(), "Negative departures found"
    print("Basic checks passed.")

if __name__ == "__main__":
    run_basic_checks()
