#!/usr/bin/env bash
set -euo pipefail

MONTH="${1:-2023-07}"
SHOCK="${2:-+10}"

echo "==> Baseline allocation for ${MONTH}"
python src/allocation.py --month "$MONTH"

python - "$MONTH" <<'PY'
import sys, pandas as pd, numpy as np
month = sys.argv[1]
df = pd.read_csv("data_work/fact_route_economics.csv")
m = df[df["month"]==month].copy()
print(f"\n=== BASELINE ({month}) ===")
print("Routes:", len(m),
      "  RASM mean:", float(np.nanmean(m["rasm"])).__round__(3),
      "  CASM mean:", float(np.nanmean(m["casm"])).__round__(3))
m.sort_values("margin", ascending=False).head(10)[["origin","dest","rasm","casm","margin"]].to_csv("data_work/top10_baseline.csv", index=False)
PY

echo "==> Applying fuel shock ${SHOCK} for ${MONTH}"
python src/sensitivity.py --month "$MONTH" --fuel "$SHOCK"

echo "==> Re-allocating with shocked financials"
python src/allocation.py --month "$MONTH" --use-shocked

python - "$MONTH" <<'PY'
import sys, pandas as pd, numpy as np
month = sys.argv[1]
df = pd.read_csv("data_work/fact_route_economics.csv")
m = df[df["month"]==month].copy()
print(f"\n=== SHOCKED ({month}) ===")
print("Routes:", len(m),
      "  RASM mean:", float(np.nanmean(m["rasm"])).__round__(3),
      "  CASM mean:", float(np.nanmean(m["casm"])).__round__(3))
m.sort_values("margin", ascending=False).head(10)[["origin","dest","rasm","casm","margin"]].to_csv("data_work/top10_shocked.csv", index=False)

# Delta summary
b = pd.read_csv("data_work/top10_baseline.csv").rename(columns={"rasm":"rasm_b","casm":"casm_b","margin":"margin_b"})
s = pd.read_csv("data_work/top10_shocked.csv").rename(columns={"rasm":"rasm_s","casm":"casm_s","margin":"margin_s"})
delta = b.merge(s, on=["origin","dest"])
delta["Δrasm"]   = (delta["rasm_s"] - delta["rasm_b"]).round(5)
delta["Δcasm"]   = (delta["casm_s"] - delta["casm_b"]).round(5)
delta["Δmargin"] = (delta["margin_s"] - delta["margin_b"]).round(0)
print("\n=== Top 10 (baseline) with deltas vs shocked ===")
print(delta[["origin","dest","rasm_b","casm_b","margin_b","Δrasm","Δcasm","Δmargin"]].to_string(index=False))
PY
