# Methodology (Draft)

## KPIs
- **RASM_route_m = Revenue_route_m / ASMs_route_m**  
- **CASM_route_m = TotalCost_route_m / ASMs_route_m**  
- **Margin_route_m = Revenue_route_m − TotalCost_route_m**

## Revenue (primary approach)
Revenue_route_m = RPMs_route_m × Yield_route_m  
(alt: Pax × AvgFare; we will choose the method with better tie-outs and document caveats.)

## Allocation Drivers (v1)
- **Fuel:** share by (BlockHours × BurnRateHr_fleet). Calibrate to Form 41 gallons monthly.  
- **Labor:** 70% BlockHours + 30% Departures.  
- **Maintenance:** BlockHours (optionally blend with Departures later).  
- **Station/Other:** 50% Departures + 50% Pax.

## Reconciliation
For each month and bucket:  
Σ allocated_cost_route_m ≈ Form41_bucket_total_m (target ±1–2%).  
Residual drift is spread proportionally across segments.

## Sensitivities
- Fuel ±10% → recompute FuelExpense_total_m and allocated costs  
- Load Factor ±2 pts → RPMs change → Revenue & RASM update

(We’ll refine weights after Week 2 calibration.)
