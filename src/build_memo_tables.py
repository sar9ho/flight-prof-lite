from pathlib import Path
import pandas as pd
import numpy as np

DATA = Path("data_work")
DOCS = Path("docs"); DOCS.mkdir(exist_ok=True)

def main(month="2023-07"):
    econ = pd.read_csv(DATA/"fact_route_economics.csv")
    m = econ[econ["month"]==month].copy()

    # Top/bottom routes by margin
    m.sort_values("margin", ascending=False).head(20).to_csv(DOCS/"top20_routes.csv", index=False)
    m.sort_values("margin", ascending=True).head(20).to_csv(DOCS/"bottom20_routes.csv", index=False)

    # Build an average distance proxy from T-100: avg pax-miles per passenger (RPMs/pax)
    seg = pd.read_csv(DATA/"fact_segments.csv", dtype={"origin":str,"dest":str})
    seg_mkt = (seg[seg["month"]==month]
               .groupby(["month","origin","dest"], as_index=False)
               .agg(RPMs=("RPMs","sum"), pax=("pax","sum"), ASMs=("ASMs","sum")))

    seg_mkt["avg_pax_miles"] = np.where(seg_mkt["pax"]>0, seg_mkt["RPMs"]/seg_mkt["pax"], np.nan)

    # Join the proxy to economics and create bins (250-mile buckets)
    mm = m.merge(seg_mkt[["origin","dest","avg_pax_miles","ASMs"]],
                 on=["origin","dest"], how="left")

    # Bin by avg_pax_miles; use broad bins to keep sample sizes healthy
    bins = [-1, 250, 500, 750, 1000, 1500, 2000, 3000, 5000]
    labels = ["<250","250-500","500-750","750-1000","1000-1500","1500-2000","2000-3000","3000-5000"]
    mm["stage_bin"] = pd.cut(mm["avg_pax_miles"], bins=bins, labels=labels)

    # ASM-weighted CASM/RASM by bin
    def wavg(s, w):
        s = s.astype(float); w = w.fillna(0.0).astype(float)
        mask = (w>0) & np.isfinite(s)
        return (s[mask]*w[mask]).sum()/w[mask].sum() if mask.any() else np.nan


    agg = (mm.groupby("stage_bin", observed=False)
            .apply(lambda x: pd.Series({
                "asm_m": x["ASMs"].sum()/1e6 if "ASMs" in x else np.nan,
                "rasm":  wavg(x["rasm"], x.get("ASMs", pd.Series(index=x.index, data=np.nan))),
                "casm":  wavg(x["casm"], x.get("ASMs", pd.Series(index=x.index, data=np.nan))),
                "routes": len(x)
            }), include_groups=False)  
            .reset_index())


    agg.to_csv(DOCS/"asm_bins_rasm_casm.csv", index=False)
    print("Wrote: docs/top20_routes.csv, docs/bottom20_routes.csv, docs/asm_bins_rasm_casm.csv")

if __name__ == "__main__":
    main()
