# flight-prof-lite

Reproducible FP&A route economics demo for Southwest-style network.

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Place BTS files in data_raw/ (T-100, Form41 P-12a & P-5.2, DB1B MARKET)
python src/ingest_data.py
python src/form41_ingest.py
python src/db1b_ingest.py
python src/allocation.py --month 2023-07

# Optional: fuel shock
python src/sensitivity.py --month 2023-07 --fuel +10
python src/allocation.py --month 2023-07
