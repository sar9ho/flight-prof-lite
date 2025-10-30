# src/ingest_data.py
from pathlib import Path
import pandas as pd

RAW = Path("data_raw")
WORK = Path("data_work"); WORK.mkdir(exist_ok=True)

def _load_one(path):
    df = pd.read_csv(path, low_memory=False)
    df.columns = [c.upper().strip() for c in df.columns]
    need = ["YEAR","MONTH","CARRIER","ORIGIN","DEST","AIRCRAFT_TYPE",
            "DEPARTURES_PERFORMED","RAMP_TO_RAMP","SEATS","PASSENGERS","DISTANCE"]
    df = df[need].copy()
    return df

def build_fact_segments(raw_files, out_csv="fact_segments.csv", carrier="WN"):
    # raw_files: list[str] or single str
    if isinstance(raw_files, str): raw_files = [raw_files]
    df = pd.concat([_load_one(RAW/f) for f in raw_files], ignore_index=True)

    # normalize and filter
    df["CARRIER"] = df["CARRIER"].astype(str).str.strip().str.upper()
    df = df[df["CARRIER"] == carrier]

    # aggregate to month–OD–aircraft
    gcols = ["YEAR","MONTH","CARRIER","ORIGIN","DEST","AIRCRAFT_TYPE"]
    df = (df.groupby(gcols, as_index=False)
            .agg({"DEPARTURES_PERFORMED":"sum","RAMP_TO_RAMP":"sum",
                  "SEATS":"sum","PASSENGERS":"sum","DISTANCE":"mean"}))

    # compute target fields
    df["month"]       = pd.to_datetime(df["YEAR"].astype(int).astype(str)+"-"+df["MONTH"].astype(int).astype(str)+"-01").dt.strftime("%Y-%m")
    df["departures"]  = df["DEPARTURES_PERFORMED"].fillna(0).astype(int)
    df["block_hours"] = (df["RAMP_TO_RAMP"].fillna(0) / 60.0)
    df["ASMs"]        = (df["SEATS"].fillna(0) * df["DISTANCE"].fillna(0)).astype(float)
    df["RPMs"]        = (df["PASSENGERS"].fillna(0) * df["DISTANCE"].fillna(0)).astype(float)
    df["pax"]         = df["PASSENGERS"].fillna(0).astype(int)
    df["fleet_type"]  = df["AIRCRAFT_TYPE"].astype(str)

    out = df[["month","ORIGIN","DEST","fleet_type","departures","block_hours","ASMs","RPMs","pax"]] \
            .rename(columns={"ORIGIN":"origin","DEST":"dest"})
    out = out[(out["ASMs"]>0) & out["month"].notna()]
    out.to_csv(WORK/out_csv, index=False)
    print(f"Wrote {WORK/out_csv} with {len(out)} rows "
          f"(years: {sorted(pd.to_datetime(out['month']).dt.year.unique())})")

if __name__ == "__main__":
    # 2023 data
    build_fact_segments(raw_files=["2023_T_T100D_SEGMENT_ALL_CARRIER.csv"])
  