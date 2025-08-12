import io
import os
import json
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

RBI_URL = "https://www.rbi.org.in/scripts/bs_viewcontent.aspx?Id=2009"
MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024
CACHE_TTL_SECONDS = 6 * 60 * 60
INDEX_PATH = os.path.join(os.path.dirname(__file__), "in_banks.json")

app = FastAPI(title="RBI API (banks / by-bank / by-ifsc)", version="2.0.0", docs_url="/")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Link list cache ----------
_links_cache: Dict[str, Any] = {"ts": 0, "data": []}

def fetch_xls_links() -> List[Dict[str, str]]:
    resp = requests.get(RBI_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith((".xls", ".xlsx")):
            url = href if href.lower().startswith(("http://", "https://")) else urljoin("https://www.rbi.org.in", href)
            title = " ".join((a.get_text() or "").split()) or url.split("/")[-1]
            items.append({"title": title, "url": url})
    if not items:
        raise HTTPException(status_code=404, detail="No Excel links found on RBI page.")
    return items

def get_cached_links() -> List[Dict[str, str]]:
    now = time.time()
    if now - _links_cache["ts"] > CACHE_TTL_SECONDS or not _links_cache["data"]:
        _links_cache["data"] = fetch_xls_links()
        _links_cache["ts"] = now
    return _links_cache["data"]

# ---------- Helpers ----------
def stream_download(url: str) -> bytes:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        total = 0
        for chunk in r.iter_content(chunk_size=16384):
            if not chunk:
                continue
            buf.write(chunk)
            total += len(chunk)
            if total > MAX_DOWNLOAD_BYTES:
                raise HTTPException(status_code=413, detail="File too large.")
        return buf.getvalue()

def detect_engine(url: str) -> str:
    return "openpyxl" if url.lower().endswith(".xlsx") else "xlrd"  # requires xlrd==1.2.0 for .xls

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_ifsc_column(cols: List[str]) -> Optional[str]:
    exact = [c for c in cols if c.strip().lower() == "ifsc"]
    if exact:
        return exact[0]
    fuzzy = [c for c in cols if "ifsc" in c.strip().lower()]
    return fuzzy[0] if fuzzy else None

def find_bank_column(cols: List[str]) -> Optional[str]:
    exact = [c for c in cols if c.strip().lower() == "bank"]
    if exact:
        return exact[0]
    fuzzy = [c for c in cols if "bank" in c.strip().lower()]
    return fuzzy[0] if fuzzy else None

# strict output keys & order
OUT_KEYS = ["BANK", "IFSC", "BRANCH", "ADDRESS", "CITY1", "CITY2", "STATE", "STD CODE", "PHONE"]

CANON_MAP = {
    "bank": "BANK", "bank name": "BANK",
    "ifsc": "IFSC", "ifsc code": "IFSC",
    "branch": "BRANCH", "branch name": "BRANCH",
    "address": "ADDRESS", "address1": "ADDRESS", "address line": "ADDRESS",
    "city": "CITY1", "city1": "CITY1", "city 1": "CITY1",
    "city2": "CITY2", "city 2": "CITY2",
    "centre": "CITY1", "district": "CITY2",
    "state": "STATE",
    "std code": "STD CODE", "std": "STD CODE", "stdcode": "STD CODE",
    "phone": "PHONE", "phone no": "PHONE", "phone number": "PHONE",
    "telephone": "PHONE", "telephone no": "PHONE",
    "contact": "PHONE", "contact no": "PHONE", "mobile": "PHONE",
}

def coerce_number_like(x):
    if pd.isna(x):
        return ""
    try:
        f = float(x)
        if f.is_integer():
            return int(f)
    except Exception:
        pass
    s = str(x).strip()
    if s.endswith(".0"):
        try:
            return int(float(s))
        except Exception:
            return s
    return s

def to_output_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    tmp = {str(k).strip().lower(): v for k, v in raw.items()}
    out: Dict[str, Any] = {k: "" for k in OUT_KEYS}
    for lk, v in tmp.items():
        if lk in CANON_MAP:
            out[CANON_MAP[lk]] = "" if pd.isna(v) else v
    if out["CITY1"] and not out["CITY2"]:
        out["CITY2"] = out["CITY1"]
    for k in ["BANK", "IFSC", "BRANCH", "STATE", "ADDRESS", "CITY1", "CITY2"]:
        out[k] = "" if pd.isna(out[k]) else str(out[k]).strip()
    out["STD CODE"] = coerce_number_like(out["STD CODE"])
    out["PHONE"] = coerce_number_like(out["PHONE"])
    out["IFSC"] = out["IFSC"].upper()
    return out

# ---------- Index build/load ----------
def build_index_file() -> List[Dict[str, Any]]:
    """
    in_banks.json:
      [{title, url, bank, ifsc_prefix}]
    bank & ifsc_prefix are taken from the FIRST ROW of the FIRST SHEET only.
    """
    items = get_cached_links()
    index: List[Dict[str, Any]] = []
    for item in items:
        try:
            data = stream_download(item["url"])
            engine = detect_engine(item["url"])
            xls = pd.ExcelFile(io.BytesIO(data), engine=engine)
            if not xls.sheet_names:
                index.append({"title": item["title"], "url": item["url"], "bank": "", "ifsc_prefix": ""})
                continue
            first_sheet = xls.sheet_names[0]
            head_df = pd.read_excel(io.BytesIO(data), sheet_name=first_sheet, engine=engine, nrows=1)
            head_df = normalize_columns(head_df)
            bank_col = find_bank_column(list(head_df.columns))
            ifsc_col = find_ifsc_column(list(head_df.columns))
            bank_val = ("" if not bank_col else str(head_df.iloc[0][bank_col]).strip().upper())
            ifsc_val = ("" if not ifsc_col else str(head_df.iloc[0][ifsc_col]).strip().upper())
            ifsc_prefix = ifsc_val[:4] if len(ifsc_val) >= 4 else ""
            index.append({"title": item["title"], "url": item["url"], "bank": bank_val, "ifsc_prefix": ifsc_prefix})
        except Exception:
            index.append({"title": item["title"], "url": item["url"], "bank": "", "ifsc_prefix": ""})
            continue
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    return index

def load_index() -> List[Dict[str, Any]]:
    if os.path.exists(INDEX_PATH):
        try:
            with open(INDEX_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return build_index_file()

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/banks")
def banks():
    """
    Return ONLY a list of {"bank": "<BANK NAME>"} from in_banks.json (build if missing).
    """
    index = load_index()
    seen = set()
    out = []
    for e in index:
        b = (e.get("bank") or "").strip().upper()
        if b and b not in seen:
            seen.add(b)
            out.append({"bank": b})
    if not out:
        raise HTTPException(status_code=404, detail="No banks found.")
    return out

@app.get("/by-bank")
def by_bank(bank: str = Query(..., description="Case-insensitive substring of the bank name")):
    """
    Bank search (index-first, mirrors IFSC flow):
      - Load in_banks.json (build if missing).
      - Find first entry whose 'bank' contains the query (case-insensitive).
      - If none, rebuild index and try once more.
      - If still none, DO NOT scan Excels; return 404 "No files matched the given bank."
      - If found, download ONLY that file and read ONLY its FIRST SHEET.
        Return rows in the required shape.
    """
    query = (bank or "").strip().upper()
    if not query:
        raise HTTPException(status_code=404, detail="No files matched the given bank.")

    index = load_index()
    candidates = [e for e in index if query in (e.get("bank") or "").upper()]

    if not candidates:
        index = build_index_file()
        candidates = [e for e in index if query in (e.get("bank") or "").upper()]
        if not candidates:
            raise HTTPException(status_code=404, detail="No files matched the given bank.")

    entry = candidates[0]
    try:
        data = stream_download(entry["url"])
        engine = detect_engine(entry["url"])
        xls = pd.ExcelFile(io.BytesIO(data), engine=engine)
        if not xls.sheet_names:
            raise HTTPException(status_code=404, detail="No files matched the given bank.")

        first_sheet = xls.sheet_names[0]
        df = pd.read_excel(io.BytesIO(data), sheet_name=first_sheet, engine=engine)
        if df.empty:
            raise HTTPException(status_code=404, detail="No files matched the given bank.")
        df = normalize_columns(df)
        rows = [to_output_row(rec) for rec in df.fillna("").to_dict(orient="records")]
        return rows
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="No files matched the given bank.")

@app.get("/by-ifsc")
def by_ifsc(ifsc: str = Query(..., description="IFSC (min length 11)")):
    """
    IFSC index-first flow:
      - Enforce length >= 11; else 404 "No rows found for the given IFSC."
      - Use IFSC prefix to pick the file from index (rebuild once if needed).
      - Search ONLY the first sheet for the exact IFSC.
    """
    code = (ifsc or "").strip().upper()
    if len(code) < 11:
        raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")
    prefix = code[:4]

    index = load_index()
    candidates = [e for e in index if e.get("ifsc_prefix") == prefix]
    if not candidates:
        index = build_index_file()
        candidates = [e for e in index if e.get("ifsc_prefix") == prefix]
        if not candidates:
            raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")

    entry = candidates[0]
    try:
        data = stream_download(entry["url"])
        engine = detect_engine(entry["url"])
        xls = pd.ExcelFile(io.BytesIO(data), engine=engine)
        if not xls.sheet_names:
            raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")

        first_sheet = xls.sheet_names[0]
        df = pd.read_excel(io.BytesIO(data), sheet_name=first_sheet, engine=engine)
        if df.empty:
            raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")
        df = normalize_columns(df)
        col = find_ifsc_column(list(df.columns))
        if not col:
            raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")
        mask = df[col].astype(str).str.upper() == code
        if not mask.any():
            raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")
        results = [to_output_row(rec) for rec in df[mask].fillna("").to_dict(orient="records")]
        return results
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="No rows found for the given IFSC.")
