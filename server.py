# server.py
import json
import os
import time
import random
import asyncio
import logging
import re
import feedparser
import PyPDF2

from fastapi import FastAPI, APIRouter, Query, Response, HTTPException
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote
from typing import Dict, List, Optional
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
CACHE_FILE = ROOT_DIR / "news_cache.json"
COMPANY_PDF = ROOT_DIR / "company_list.pdf"

BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60
SHEET_CHUNK_SIZE = 5000

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_TAB_NAME = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")

# ---------------------------------------------------
# GLOBAL STATE
# ---------------------------------------------------
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}

INDEX_NEWS_KEYS = ["nifty", "sensex", "banknifty", "nifty bank", "index"]

TOP_STOCKS = [
    "Reliance Industries Limited",
    "Tata Consultancy Services Limited",
    "HDFC Bank Limited",
    "ICICI Bank Limited",
    "Infosys Limited",
    "Hindustan Unilever Limited",
    "State Bank of India",
    "Larsen & Toubro Limited",
    "Bharti Airtel Limited",
    "ITC Limited",
    "Tata Motors Limited",
    "Kotak Mahindra Bank Limited",
    "Axis Bank Limited",
    "Maruti Suzuki India Limited",
    "Bajaj Finance Limited",
    "Mahindra & Mahindra Limited",
    "Wipro Limited",
    "Power Grid Corporation of India Limited",
    "Asian Paints Limited",
    "HCL Technologies Limited"
]

SECTOR_KEYWORDS = {
    "FMCG": ["fmcg", "food", "beverage", "consumer goods", "packaged", "retail"],
    "HEALTH": ["pharma", "hospital", "healthcare", "vaccine", "biotech", "drug"],
    "IT": ["software", "it", "technology", "digital", "tcs", "infosys", "wipro"],
    "BANKING": ["bank", "banking", "hdfc", "icici", "sbi", "kotak", "axis"],
    "AUTO": ["auto", "automobile", "vehicle", "motors", "maruti", "tata motors"],
    "METALS": ["steel", "metal", "mining", "ore"],
    "ENERGY": ["oil", "energy", "gas", "petro", "bpcl", "hpcl", "oil and gas"],
    "PSU": ["psu", "public sector"],
    "TELECOM": ["telecom", "airtel", "vodafone", "jio"],
    "MIDCAP": ["midcap"],
    "SMALLCAP": ["smallcap"],
    "FINANCE": ["finance", "nbfc", "lending", "bajaj finance"],
    "INDEX": ["index", "nifty", "sensex", "bank nifty"]
}

GOOD_KEYWORDS = [
    "profit", "record", "growth", "surge", "beats", "upgrade", "wins",
    "strong", "rise", "positive", "acquisition", "expansion"
]

BAD_KEYWORDS = [
    "loss", "fraud", "scam", "crash", "decline", "penalty", "investigation",
    "downgrade", "fall", "weak", "slump", "lawsuit"
]

IMPACT_KEYWORDS = GOOD_KEYWORDS + BAD_KEYWORDS + [
    "earnings", "results", "investment", "sebi", "revenue"
]

# ---------------------------------------------------
# UTILITIES
# ---------------------------------------------------
def clean_html(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def detect_sentiment(text: str) -> str:
    if not text:
        return "neutral"
    t = text.lower()
    for w in GOOD_KEYWORDS:
        if w in t:
            return "good"
    for w in BAD_KEYWORDS:
        if w in t:
            return "bad"
    return "neutral"

def remove_duplicates(news_list: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for n in news_list:
        key = (n.get("title", "").strip().lower(), n.get("link", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out

def parse_date(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        try:
            return datetime.fromisoformat(value)
        except:
            return None

def normalize_news_cache():
    for company, data in list(NEWS_CACHE.items()):
        ts = data.get("timestamp")
        if isinstance(ts, datetime):
            data["timestamp"] = ts.timestamp()

        for item in data.get("news", []):
            pub = item.get("pubDate")
            parsed = parse_date(str(pub))
            item["pubDate"] = parsed.isoformat() if parsed else (pub or "")
# ---------------------------------------------------
# GOOGLE SHEETS INTEGRATION (single-row upsert + chunk writer)
# ---------------------------------------------------
def get_quoted_tab(tab_name: str) -> str:
    safe = tab_name.replace("'", "''")
    return f"'{safe}'"

def get_sheets_service():
    creds_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
    if not creds_json:
        logger.error("Google Sheets credentials JSON missing (GOOGLE_SHEETS_CREDENTIALS_JSON).")
        return None
    try:
        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return build("sheets", "v4", credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to create Google Sheets service: {e}")
        return None

def clear_sheet(service):
    if not service or not SHEET_ID:
        logger.warning("Skipping clear_sheet: service or SHEET_ID missing.")
        return
    tab = get_quoted_tab(SHEET_TAB_NAME)
    range_to_clear = f"{tab}!A2:E"
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID,
            range=range_to_clear,
            body={}
        ).execute()
        logger.info(f"Cleared sheet range: {range_to_clear}")
    except HttpError as he:
        logger.error(f"HttpError clearing sheet: {he.status_code} - {getattr(he, 'error_details', he)}")
    except Exception as e:
        logger.error(f"Error clearing sheet: {e}")

def write_rows_chunk(service, rows, start_row=2):
    """
    Write a list of rows starting at start_row (1-based). Uses update at the specified start.
    Columns: Company | Title | Link | PubDate | Sentiment
    """
    if not service or not SHEET_ID or not rows:
        return
    tab = get_quoted_tab(SHEET_TAB_NAME)
    range_to_write = f"{tab}!A{start_row}"
    body = {"values": rows, "majorDimension": "ROWS"}
    try:
        resp = service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=range_to_write,
            valueInputOption="RAW",
            body=body
        ).execute()
        logger.info(f"Wrote chunk to {range_to_write} rows={len(rows)}; updatedCells={resp.get('updatedCells')}")
    except HttpError as he:
        logger.error(f"HttpError writing chunk to sheet: {he.status_code} - {getattr(he, 'error_details', he)}")
        raise
    except Exception as e:
        logger.error(f"Error writing chunk to sheet: {e}")
        raise

def append_rows(service, rows):
    """Append rows at the end of sheet (used as fallback)."""
    if not service or not SHEET_ID or not rows:
        return
    tab = get_quoted_tab(SHEET_TAB_NAME)
    range_to_append = f"{tab}!A2"
    body = {"values": rows}
    try:
        resp = service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=range_to_append,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body
        ).execute()
        logger.info(f"Appended rows count={len(rows)}; updates={resp.get('updates')}")
    except Exception as e:
        logger.error(f"Error appending rows: {e}")
        raise

def upsert_company_row(service, company: str, row_values: List[str]) -> None:
    """
    Find first occurrence of `company` in column A (A2:A...) and replace that row.
    If not found, append to the end.
    row_values must be a list of column values [company, title, link, pubDate, sentiment]
    """
    if not service or not SHEET_ID:
        return
    tab = get_quoted_tab(SHEET_TAB_NAME)
    try:
        # Read column A to find company
        read_range = f"{tab}!A2:A10000"
        resp = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=read_range).execute()
        values = resp.get("values", [])
        # find index
        found_idx = None
        for idx, v in enumerate(values):  # idx starts at 0 => sheet row is idx+2
            if v and v[0].strip().lower() == company.strip().lower():
                found_idx = idx
                break
        if found_idx is not None:
            sheet_row = 2 + found_idx
            write_rows_chunk(service, [row_values], start_row=sheet_row)
            logger.info(f"Upserted company '{company}' at row {sheet_row}")
        else:
            # append
            append_rows(service, [row_values])
            logger.info(f"Appended company '{company}' to sheet")
    except HttpError as he:
        logger.error(f"HttpError in upsert_company_row: {he.status_code} - {getattr(he, 'error_details', he)}")
        raise
    except Exception as e:
        logger.error(f"Error in upsert_company_row: {e}")
        raise

def write_news_to_sheet(all_news_rows):
    """
    Writes flattened rows to Google Sheet in chunks.
    Each row format: [company, title, link, pubDate, sentiment]
    """
    service = get_sheets_service()
    if not service or not SHEET_ID:
        logger.warning("Skipping write_news_to_sheet: service or SHEET_ID missing.")
        return
    if not all_news_rows:
        logger.info("No rows to write to Google Sheet.")
        return

    try:
        # Clear and write in chunks
        clear_sheet(service)
        total = len(all_news_rows)
        logger.info(f"Writing {total} rows to Google Sheet in chunks of {SHEET_CHUNK_SIZE}...")
        written = 0
        start_row = 2
        while written < total:
            chunk = all_news_rows[written:written + SHEET_CHUNK_SIZE]
            write_rows_chunk(service, chunk, start_row=start_row + written)
            written += len(chunk)
        logger.info("Finished writing all chunks to Google Sheet.")
    except Exception as e:
        logger.error(f"Error writing to sheet: {e}")

# ---------------------------------------------------
# FLATTEN CACHE -> sheet rows (no description)
# ---------------------------------------------------
def flatten_all_news():
    """
    produce rows : [company, title, link, pubDate, sentiment]
    Additionally will produce at the end:
      - rows for 'ALL' (feed from build_all_section)
      - rows for each sector (top item)
    """
    rows = []
    # Primary company rows
    for company, data in NEWS_CACHE.items():
        for item in data.get("news", []):
            rows.append([
                company,
                item.get("title", ""),
                item.get("link", ""),
                item.get("pubDate", ""),
                item.get("sentiment", "")
            ])

    # Add 'ALL' section (latest 50 items from build_all_section)
    try:
        all_items = build_all_section(limit=50, days=None, only_impact=False)
        for it in all_items:
            rows.append([
                "ALL",
                f"{it.get('company','')} — {it.get('title','')}",
                it.get("link", ""),
                it.get("pubDate", ""),
                it.get("sentiment", "")
            ])
    except Exception as e:
        logger.error(f"Error building ALL section for sheet: {e}")

    # Add sectors: pick top 1 item per sector
    try:
        for sector, keys in SECTOR_KEYWORDS.items():
            sec_items = build_sector_section(keys, limit=5, days=None)
            if sec_items:
                top = sec_items[0]
                rows.append([
                    sector,
                    f"{top.get('company','')} — {top.get('title','')}",
                    top.get("link", ""),
                    top.get("pubDate", ""),
                    top.get("sentiment", "")
                ])
    except Exception as e:
        logger.error(f"Error building sector rows for sheet: {e}")

    return rows

# ---------------------------------------------------
# LOAD companies from PDF
# ---------------------------------------------------
def load_company_names():
    global COMPANY_NAMES
    fallback_path = Path("/mnt/data/combined_companies.pdf")
    pdf_path = COMPANY_PDF if COMPANY_PDF.exists() else (fallback_path if fallback_path.exists() else None)

    if pdf_path is None:
        logger.error(f"Company PDF missing at {COMPANY_PDF} and no fallback found. COMPANY_NAMES will be empty.")
        return

    try:
        with open(pdf_path, "rb") as f:
            pdf = PyPDF2.PdfReader(f)
            text = "".join([(page.extract_text() or "") for page in pdf.pages])
        companies = []
        for line in text.split("\n"):
            line = line.strip()
            if line and ("Limited" in line or "Ltd" in line or "ETF" in line or "Corporation" in line or "Industries" in line):
                companies.append(line)
        seen = set()
        COMPANY_NAMES = [c for c in companies if not (c in seen or seen.add(c))]
        logger.info(f"Loaded {len(COMPANY_NAMES)} companies from PDF: {pdf_path}")
    except Exception as e:
        logger.error(f"Error loading company PDF ({pdf_path}): {e}")

# ---------------------------------------------------
# PERSISTENT CACHE load/save
# ---------------------------------------------------
def load_cache_from_file():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    # simple assignment preserved
                    NEWS_CACHE.update(data)
            logger.info(f"Loaded cache file: {len(NEWS_CACHE)} companies")
        except Exception as e:
            logger.error(f"Failed to load cache file: {e}")

async def save_cache_periodically():
    while True:
        try:
            normalize_news_cache()
            with open(CACHE_FILE, "w") as f:
                json.dump(NEWS_CACHE, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"Saved cache to file ({len(NEWS_CACHE)} companies)")
        except Exception as e:
            logger.error(f"Error saving cache file: {e}")
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)

# ---------------------------------------------------
# RSS fetching (now limited to 1 latest item)
# ---------------------------------------------------
async def fetch_company_news(company_name: str) -> List[Dict]:
    """
    Query news.google.com RSS for "<company_name> stock" and return up to 1 latest cleaned item.
    We intentionally keep only title, link, pubDate (no description) to reduce size.
    """
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        feed = await asyncio.to_thread(feedparser.parse, url)
        news_items = []
        for entry in feed.entries[:6]:
            title = clean_html(entry.get("title", "") or "")
            link = entry.get("link", "") or entry.get("id", "")
            pubDate = entry.get("published", "") or entry.get("updated", "") or ""
            text_combined = title.strip()
            news_items.append({
                "title": title,
                "link": link,
                "pubDate": pubDate,
                "raw_text": text_combined
            })
        news_items = remove_duplicates(news_items)
        # parse dates to iso
        for n in news_items:
            p = n.get("pubDate", "")
            parsed = parse_date(p)
            if parsed:
                n["pubDate"] = parsed.isoformat()
        # return only the single most recent item (if any)
        if news_items:
            # try sort by pubDate descending if present
            try:
                news_items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
            except Exception:
                pass
            return [news_items[0]]
        return []
    except Exception as e:
        logger.error(f"fetch error for {company_name}: {e}")
        return []

# ---------------------------------------------------
# Update single company: cache + immediate sheet upsert
# ---------------------------------------------------
async def update_one_company(company: str):
    try:
        news = await fetch_company_news(company)
        # compute sentiment on title only
        for n in news:
            txt = n.get("title", "") or ""
            n["sentiment"] = detect_sentiment(txt)
            n["mentioned_companies"] = []
        timestamp = time.time()

        # store only 1 item per company (list of length 0 or 1)
        if news:
            # keep newest item only
            NEWS_CACHE[company] = {"news": news, "timestamp": timestamp}
        else:
            if company not in NEWS_CACHE:
                NEWS_CACHE[company] = {"news": [], "timestamp": timestamp}

        # Immediately upsert this company's row into Google Sheet (single-row)
        try:
            service = get_sheets_service()
            if service and SHEET_ID and news:
                n = news[0]
                row = [
                    company,
                    n.get("title", ""),
                    n.get("link", ""),
                    n.get("pubDate", ""),
                    n.get("sentiment", "")
                ]
                upsert_company_row(service, company, row)
        except Exception as e:
            logger.error(f"Failed to upsert company row for {company}: {e}")

    except Exception as e:
        logger.error(f"update_one_company error for {company}: {e}")

# ---------------------------------------------------
# Concurrency batch
# ---------------------------------------------------
async def update_batch(companies: List[str]):
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    async def worker(c):
        async with sem:
            await update_one_company(c)
    await asyncio.gather(*[worker(c) for c in companies], return_exceptions=True)
# ---------------------------------------------------
# BACKGROUND UPDATE LOOP (NO REST — continuous cycles)
# ---------------------------------------------------
async def background_news_updater():
    logger.info("Background updater started (continuous, no resting)")

    while True:
        try:
            total = len(COMPANY_NAMES)
            if total == 0:
                logger.info("No companies loaded yet, sleeping 10s...")
                await asyncio.sleep(10)
                continue

            # cycle start
            logger.info(f"Starting full update cycle for {total} companies")

            for i in range(0, total, BATCH_SIZE):
                batch = COMPANY_NAMES[i:i + BATCH_SIZE]
                logger.info(
                    f"Updater: processing batch {i // BATCH_SIZE + 1} / {(total + BATCH_SIZE - 1) // BATCH_SIZE} "
                    f"({len(batch)} companies)"
                )
                await update_batch(batch)
                # small pause to avoid hammering Google servers
                await asyncio.sleep(random.uniform(0.5, 1.2))

            logger.info("Full cycle finished.")

            # After cycle: regenerate ALL + sector rows only (not needed because upsert handles company rows)
            try:
                service = get_sheets_service()
                if service:
                    logger.info("Updating ALL + SECTOR rows...")
                    rows = flatten_all_news()  # includes company rows + ALL rows + sector rows
                    write_news_to_sheet(rows)
                    logger.info("ALL + Sectors updated in sheet!")
            except Exception as e:
                logger.error(f"Error regenerating ALL/SECTOR sheet rows: {e}")

            # Immediately start next cycle (NO sleeping)
            logger.info("Restarting cycle immediately (no rest)...")

        except Exception as e:
            logger.error(f"Background updater crashed: {e}")
            await asyncio.sleep(5)


# ---------------------------------------------------
# FILTER HELPERS
# ---------------------------------------------------
def is_high_impact(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in IMPACT_KEYWORDS)

def within_days(item: Dict, days: int) -> bool:
    if not days:
        return True
    pub = item.get("pubDate", "")
    if not pub:
        return False
    dt = parse_date(pub)
    if not dt:
        return True
    now = datetime.now(timezone.utc)
    delta = now - dt
    return delta.days <= days


# ---------------------------------------------------
# SECTION BUILDERS (same logic, but description removed)
# ---------------------------------------------------
def build_index_section(limit=50, days: Optional[int] = None):
    results = []
    added = set()
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "")).lower()
            if any(k in txt for k in INDEX_NEWS_KEYS):
                if days and not within_days(n, days):
                    continue
                item = n.copy()
                item["company"] = company
                results.append(item)
                added.add((company, item.get("title", "")))
                break
        if len(results) >= limit:
            break
    results = remove_duplicates(results)
    try:
        results.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except:
        pass
    return results[:limit]

def build_largecap_section(limit=60, days: Optional[int] = None):
    items = []
    for top in TOP_STOCKS:
        for n in NEWS_CACHE.get(top, {}).get("news", []):
            if days and not within_days(n, days):
                continue
            x = n.copy()
            x["company"] = top
            items.append(x)
            break
        if len(items) >= limit:
            break
    items = remove_duplicates(items)
    try:
        items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except:
        pass
    return items[:limit]

def build_general_section(limit=150, days: Optional[int] = None):
    all_items = []
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            if days and not within_days(n, days):
                continue
            x = n.copy()
            x["company"] = company
            all_items.append(x)
    all_items = remove_duplicates(all_items)
    try:
        all_items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except:
        pass
    return all_items[:limit]

def build_all_section(limit=150, days: Optional[int] = None, only_impact=False):
    results = []
    added = set()

    # index first
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "")).lower()
            if any(k in txt for k in INDEX_NEWS_KEYS):
                if days and not within_days(n, days):
                    continue
                item = n.copy()
                item["company"] = company
                results.append(item)
                added.add(company)
                break

    # top stocks next
    for top in TOP_STOCKS:
        if top in NEWS_CACHE and top not in added:
            candidates = NEWS_CACHE[top].get("news", [])
            candidates = sorted(
                candidates,
                key=lambda it:
                    (not is_high_impact(it.get("title", "")),
                     it.get("pubDate", "")),
                reverse=False
            )
            for cand in candidates:
                if days and not within_days(cand, days):
                    continue
                if only_impact and not is_high_impact(cand.get("title", "")):
                    continue
                chosen = cand
                x = chosen.copy()
                x["company"] = top
                results.append(x)
                added.add(top)
                break

    # impactful others
    for company, cache in NEWS_CACHE.items():
        if company in added:
            continue
        for n in cache.get("news", []):
            if days and not within_days(n, days):
                continue
            if only_impact and not is_high_impact(n.get("title", "")):
                continue
            if is_high_impact(n.get("title", "")):
                x = n.copy()
                x["company"] = company
                results.append(x)
                added.add(company)
                break
        if len(results) >= limit:
            break

    # fill rest
    if len(results) < limit:
        for company, cache in NEWS_CACHE.items():
            if company in added:
                continue
            for n in cache.get("news", []):
                if days and not within_days(n, days):
                    continue
                if only_impact and not is_high_impact(n.get("title", "")):
                    continue
                x = n.copy()
                x["company"] = company
                results.append(x)
                added.add(company)
                break
            if len(results) >= limit:
                break

    results = remove_duplicates(results)

    try:
        results.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except:
        pass

    return results[:limit]


# ---------------------------------------------------
# RESULTS / SECTOR / PENNY builders
# ---------------------------------------------------
def build_results_section(limit=150, days: Optional[int] = None):
    results = []
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "")).lower()
            if any(w in txt for w in [
                "q1","q2","q3","q4","quarter",
                "quarterly","annual","yearly",
                "results","earnings","net profit","revenue","pat","eps"
            ]):
                if days and not within_days(n, days):
                    continue
                x = n.copy()
                x["company"] = company
                results.append(x)
                break
        if len(results) >= limit:
            break
    try: results.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except: pass
    return remove_duplicates(results)[:limit]

def build_sector_section(keywords: List[str], limit=150, days: Optional[int] = None):
    items = []
    keys = [k.lower() for k in keywords]
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "")).lower()
            if any(k in txt for k in keys):
                if days and not within_days(n, days):
                    continue
                x = n.copy()
                x["company"] = company
                items.append(x)
    items = remove_duplicates(items)
    try: items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except: pass
    return items[:limit]

def build_penny_section(limit=150, days: Optional[int] = None):
    items = []
    for p in PENNY_STOCKS:
        for n in NEWS_CACHE.get(p, {}).get("news", []):
            if days and not within_days(n, days):
                continue
            x = n.copy()
            x["company"] = p
            items.append(x)
    items = remove_duplicates(items)
    try: items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except: pass
    return items[:limit]


# ---------------------------------------------------
# API ENDPOINTS (same logic, no description fields)
# ---------------------------------------------------
@api_router.get("/companies/search")
async def search_companies(q: str = Query("", description="Search query")):
    if not q:
        return []
    ql = q.lower()
    matches = [name for name in COMPANY_NAMES if name.lower().startswith(ql)]
    if not matches:
        matches = [name for name in COMPANY_NAMES if ql in name.lower()]
    return matches[:50]

@api_router.get("/news/company/{company_name}")
async def get_company_news(company_name: str):
    news = NEWS_CACHE.get(company_name, {}).get("news", [])
    for n in news:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title", ""))
    return {"company": company_name, "news": news}
# ---------------------------------------------------
# /news/all endpoint (description removed + fully compatible)
# ---------------------------------------------------
@api_router.get("/news/all")
async def get_all_news(
    days: Optional[int] = Query(None),
    only_impact: Optional[bool] = Query(False),
    include_indexes: Optional[bool] = Query(False),
    same_day: Optional[bool] = Query(False)
):
    if not same_day:
        items = build_all_section(limit=150, days=days, only_impact=only_impact)
        for n in items:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", ""))
        if include_indexes:
            indexes = build_index_section(limit=60, days=days)
            largecap = build_largecap_section(limit=60, days=days)
            general = build_general_section(limit=150, days=days)
            for arr in (indexes, largecap, general):
                for n in arr:
                    if "sentiment" not in n:
                        n["sentiment"] = detect_sentiment(n.get("title", ""))
            return {
                "sections": {
                    "indexes": indexes,
                    "largecap": largecap,
                    "general": general
                },
                "count": len(indexes) + len(largecap) + len(general)
            }
        return {"news": items, "count": len(items)}

    # SAME-DAY logic
    today_days = 0
    yesterday_days = 1

    indexes = build_index_section(limit=60, days=today_days)
    largecap = build_largecap_section(limit=60, days=today_days)
    general = build_general_section(limit=150, days=today_days)

    if not indexes:
        combined_today = remove_duplicates((largecap or []) + (general or []))
        if combined_today:
            flat_today = combined_today[:150]
            for n in flat_today:
                if "sentiment" not in n:
                    n["sentiment"] = detect_sentiment(n.get("title", ""))
            if include_indexes:
                return {
                    "sections": {
                        "indexes": [],
                        "largecap": largecap,
                        "general": general
                    },
                    "count": len((largecap or [])) + len((general or []))
                }
            return {"news": flat_today, "count": len(flat_today)}

    if indexes:
        if include_indexes:
            for arr in (indexes, largecap, general):
                for n in arr:
                    if "sentiment" not in n:
                        n["sentiment"] = detect_sentiment(n.get("title", ""))
            return {
                "sections": {
                    "indexes": indexes,
                    "largecap": largecap,
                    "general": general
                },
                "count": len(indexes) + len(largecap) + len(general)
            }
        flat = remove_duplicates(indexes + largecap + general)[:150]
        for n in flat:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", ""))
        return {"news": flat, "count": len(flat)}

    # fallback on yesterday
    indexes_y = build_index_section(limit=60, days=yesterday_days)
    largecap_y = build_largecap_section(limit=60, days=yesterday_days)
    general_y = build_general_section(limit=150, days=yesterday_days)

    if not (indexes_y or largecap_y or general_y):
        fallback_items = build_all_section(limit=150, days=None, only_impact=only_impact)
        for n in fallback_items:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", ""))
        if include_indexes:
            idx = build_index_section(limit=60, days=None)
            lc = build_largecap_section(limit=60, days=None)
            gen = build_general_section(limit=150, days=None)
            for arr in (idx, lc, gen):
                for n in arr:
                    if "sentiment" not in n:
                        n["sentiment"] = detect_sentiment(n.get("title", ""))
            return {
                "sections": {
                    "indexes": idx,
                    "largecap": lc,
                    "general": gen
                },
                "count": len(idx) + len(lc) + len(gen)
            }
        return {"news": fallback_items, "count": len(fallback_items)}

    if include_indexes:
        for arr in (indexes_y, largecap_y, general_y):
            for n in arr:
                if "sentiment" not in n:
                    n["sentiment"] = detect_sentiment(n.get("title", ""))
        return {
            "sections": {
                "indexes": indexes_y,
                "largecap": largecap_y,
                "general": general_y
            },
            "count": len(indexes_y) + len(largecap_y) + len(general_y)
        }

    flat_y = remove_duplicates((indexes_y or []) + (largecap_y or []) + (general_y or []))[:150]
    for n in flat_y:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title", ""))
    return {"news": flat_y, "count": len(flat_y)}


# ---------------------------------------------------
# SECTOR NEWS
# ---------------------------------------------------
@api_router.get("/news/sector/{sector_name}")
async def get_sector_news(sector_name: str, days: Optional[int] = Query(None)):
    s = sector_name.upper()
    if s == "PENNY":
        items = build_penny_section(days=days)
    elif s in ("LARGECAP", "LARGE CAP"):
        items = build_largecap_section(days=days)
    elif s == "MIDCAP":
        items = build_sector_section(["midcap"], days=days)
    elif s == "SMALLCAP":
        items = build_sector_section(["smallcap"], days=days)
    else:
        keywords = SECTOR_KEYWORDS.get(s, [sector_name])
        items = build_sector_section(keywords, days=days)

    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title", ""))
    return {"news": items, "count": len(items)}


# ---------------------------------------------------
# STATUS + PING
# ---------------------------------------------------
@api_router.get("/status")
async def get_status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE),
        "sheet": SHEET_ID,
        "tab": SHEET_TAB_NAME
    }

@api_router.get("/ping")
async def ping():
    return {"alive": True, "time": time.time()}


# ---------------------------------------------------
# DEBUG ENDPOINTS
# ---------------------------------------------------
@api_router.get("/debug/push_sheet_test")
async def debug_push_sheet_test():
    try:
        for company, data in NEWS_CACHE.items():
            if data.get("news"):
                n = data["news"][0]
                row = [[
                    company,
                    n.get("title", ""),
                    n.get("link", ""),
                    n.get("pubDate", ""),
                    n.get("sentiment", detect_sentiment(n.get("title", "")))
                ]]
                service = get_sheets_service()
                clear_sheet(service)
                write_rows_chunk(service, row, start_row=2)
                return {"ok": True, "company": company}
        return {"ok": False, "msg": "No news yet"}
    except Exception as e:
        raise HTTPException(500, str(e))


@api_router.get("/debug/run_once")
async def debug_run_once(limit: int = 10):
    try:
        to_process = COMPANY_NAMES[:limit]
        await update_batch(to_process)
        rows = []
        for c in to_process:
            for n in NEWS_CACHE.get(c, {}).get("news", []):
                rows.append([
                    c,
                    n.get("title", ""),
                    n.get("link", ""),
                    n.get("pubDate", ""),
                    n.get("sentiment", "")
                ])
        write_news_to_sheet(rows)
        return {"ok": True, "count": len(rows)}
    except Exception as e:
        raise HTTPException(500, str(e))


@api_router.get("/debug/write_all")
async def debug_write_all():
    try:
        rows = flatten_all_news()
        write_news_to_sheet(rows)
        return {"ok": True, "rows": len(rows)}
    except Exception as e:
        raise HTTPException(500, str(e))


# ---------------------------------------------------
# ROOT + MIDDLEWARE
# ---------------------------------------------------
@app.get("/")
async def root():
    return Response(
        "<html><body><h2>StockPulse Backend Running</h2></body></html>",
        media_type="text/html"
    )


app.include_router(api_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


# ---------------------------------------------------
# STARTUP + SHUTDOWN
# ---------------------------------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("Startup: loading companies + cache...")
    load_company_names()
    load_cache_from_file()
    asyncio.create_task(background_news_updater())
    asyncio.create_task(save_cache_periodically())
    logger.info("Startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutdown: saving cache...")
    try:
        normalize_news_cache()
        with open(CACHE_FILE, "w") as f:
            json.dump(NEWS_CACHE, f, indent=2)
        logger.info("Saved cache.")
    except:
        logger.error("Error saving cache.")
