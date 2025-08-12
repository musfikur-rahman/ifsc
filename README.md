# RBI IFSC API

A FastAPI service that fetches official Excel files linked from the Reserve Bank of India (RBI) and exposes three endpoints:

- `GET /` (API docs)
- `GET /health` — health check
- `GET /banks` — list of available banks
- `GET /by-bank?bank=STATE%20BANK%20OF%20INDIA` — all rows for the first file that matches the bank name
- `GET /by-ifsc?ifsc=XXXX0YYYYYY` — details for an exact IFSC code (min length 11)

## How it works (high level)

- Scrapes the RBI content page at `https://www.rbi.org.in/scripts/bs_viewcontent.aspx?Id=2009` for `.xls` and `.xlsx` links.
- Builds a local index file `bd_banks.json` on first run to map bank names and IFSC prefixes to their source file.
- Only the **first sheet** of each Excel is read, and downloads are capped to 25 MB for safety.
- Results are normalized to consistent keys: `BANK, IFSC, BRANCH, ADDRESS, CITY1, CITY2, STATE, STD CODE, PHONE`.

> Note: This project fetches publicly accessible files from RBI at runtime; it does **not** redistribute the data. Accuracy depends on the RBI source files.

## Quickstart

### 1) Local (Python)

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run (reload for dev)
uvicorn app.rbi_api:app --reload --port 8000
