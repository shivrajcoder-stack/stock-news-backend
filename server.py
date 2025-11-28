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

# -----------------------------
# Config
# -----------------------------
CACHE_FILE = ROOT_DIR / "news_cache.json"
COMPANY_PDF = ROOT_DIR / "company_list.pdf"
CACHE_DURATION = 90 * 60  # not used as long sleep anymore; cycle rest is minimal
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60
SHEET_CHUNK_SIZE = 5000  # rows per chunk when writing to Google Sheets

# Google Sheets config (from env)
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")  # spreadsheet id
SHEET_TAB_NAME = os.getenv("GOOGLE_SHEET_TAB", "Sheet1")  # tab name; default Sheet1

# -----------------------------
# Global State
# -----------------------------
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

PENNY_STOCKS = ["Tilaknagar Industries Limited", "3i Infotech Limited", "XYZ Penny Ltd"]

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

# -----------------------------
# Utilities
# -----------------------------
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

# -----------------------------
# Google Sheets Integration
# -----------------------------
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
    range_to_clear = f"{tab}!A2:E"  # we only use columns A..E now (company,title,link,pubDate,sentiment)
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

# -----------------------------
# Helpers to flatten/write per-batch
# -----------------------------
def flatten_company_rows_for_write(company: str, news_items: List[Dict], keep=5):
    """
    Return rows for a single company (list of lists).
    Note: DESCRIPTION removed from sheet (we keep it in cache but not in sheet).
    Columns: company, title, link, pubDate, sentiment
    Sorted newest -> oldest by pubDate.
    """
    # sort news_items by parsed pubDate desc (if possible)
    def parse_key(it):
        dt = parse_date(it.get("pubDate") or "")
        return dt.timestamp() if dt else 0
    items = sorted(news_items or [], key=parse_key, reverse=True)[:keep]
    rows = []
    for n in items:
        rows.append([
            company,
            n.get("title", ""),
            n.get("link", ""),
            n.get("pubDate", ""),
            n.get("sentiment", detect_sentiment(n.get("title", "") + " " + n.get("description", "")))
        ])
    return rows

# -----------------------------
# Load companies from PDF
# -----------------------------
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

# -----------------------------
# Persistent cache load/save
# -----------------------------
def load_cache_from_file():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
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

# -----------------------------
# RSS fetching (cache-first rule-based)
# -----------------------------
async def fetch_company_news(company_name: str) -> List[Dict]:
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        feed = await asyncio.to_thread(feedparser.parse, url)
        news_items = []
        for entry in feed.entries[:8]:
            title = clean_html(entry.get("title", "") or "")
            summary = clean_html(entry.get("summary", "") or entry.get("description", "") or "")
            link = entry.get("link", "") or entry.get("id", "")
            pubDate = entry.get("published", "") or entry.get("updated", "") or ""
            text_combined = (title + " " + summary).strip()
            news_items.append({
                "title": title,
                "description": summary,  # kept in cache but not written to sheet
                "link": link,
                "pubDate": pubDate,
                "raw_text": text_combined
            })
        news_items = remove_duplicates(news_items)
        for n in news_items:
            p = n.get("pubDate", "")
            parsed = parse_date(p)
            if parsed:
                n["pubDate"] = parsed.isoformat()
        return news_items[:5]
    except Exception as e:
        logger.error(f"fetch error for {company_name}: {e}")
        return []

# -----------------------------
# Update single company
# -----------------------------
async def update_one_company(company: str):
    try:
        news = await fetch_company_news(company)
        for n in news:
            txt = (n.get("title", "") + " " + n.get("description", "")).strip()
            n["sentiment"] = detect_sentiment(txt)
            n["mentioned_companies"] = []
        timestamp = time.time()
        if news:
            # store up to 5 in cache
            NEWS_CACHE[company] = {"news": news[:5], "timestamp": timestamp}
        elif company not in NEWS_CACHE:
            NEWS_CACHE[company] = {"news": [], "timestamp": timestamp}
        return company, news[:5]
    except Exception as e:
        logger.error(f"update_one_company error for {company}: {e}")
        return company, []

# -----------------------------
# Concurrency batch
# -----------------------------
async def update_batch(companies: List[str]):
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    async def worker(c):
        async with sem:
            return await update_one_company(c)
    results = await asyncio.gather(*[worker(c) for c in companies], return_exceptions=True)
    # return list of tuples (company, news_list) for writers
    processed = []
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"batch worker exception: {r}")
        elif isinstance(r, tuple) and len(r) == 2:
            processed.append(r)
    return processed

# -----------------------------
# Section builders used for All/Sector summary rows
# -----------------------------
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

def build_all_section(limit=150, days: Optional[int] = None, only_impact=False):
    results = []
    added = set()
    # index-like news first
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "") + " " + n.get("description", "")).lower()
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
                key=lambda it: (not is_high_impact(it.get("title", "") + " " + it.get("description", "")), it.get("pubDate", "")),
                reverse=False
            )
            for cand in candidates:
                if days and not within_days(cand, days):
                    continue
                if only_impact and not is_high_impact(cand.get("title", "") + " " + cand.get("description", "")):
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
            if only_impact and not is_high_impact(n.get("title", "") + " " + n.get("description", "")):
                continue
            if is_high_impact(n.get("title", "") + " " + n.get("description", "")):
                x = n.copy()
                x["company"] = company
                results.append(x)
                added.add(company)
                break
        if len(results) >= limit:
            break
    # fill with recent / diverse if needed
    if len(results) < limit:
        for company, cache in NEWS_CACHE.items():
            if company in added:
                continue
            for n in cache.get("news", []):
                if days and not within_days(n, days):
                    continue
                if only_impact and not is_high_impact(n.get("title", "") + " " + n.get("description", "")):
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
    except Exception:
        pass
    return results[:limit]

def build_sector_top_item(sector_key: str):
    keywords = SECTOR_KEYWORDS.get(sector_key, [sector_key])
    items = []
    keys = [k.lower() for k in keywords]
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "") + " " + n.get("description", "")).lower()
            if any(k in txt for k in keys):
                items.append((company, n))
    if not items:
        return None
    # pick newest
    items_sorted = sorted(items, key=lambda t: parse_date(t[1].get("pubDate", "")) or datetime(1970,1,1), reverse=True)
    company, n = items_sorted[0]
    return {"company": company, "item": n}

# -----------------------------
# Background updater (incremental sheet writes)
# -----------------------------
async def background_news_updater():
    logger.info("Background updater started")
    service = None
    while True:
        try:
            total = len(COMPANY_NAMES)
            if total == 0:
                logger.info("No companies loaded yet, sleeping before retrying...")
                await asyncio.sleep(10)
                continue

            # Acquire sheets service and clear sheet at cycle start
            service = get_sheets_service()
            if service and SHEET_ID:
                try:
                    clear_sheet(service)
                except Exception as e:
                    logger.error(f"Failed to clear sheet at cycle start: {e}")

            write_pointer = 2  # write into A2 and onward
            # process in batches and after each batch write that batch's rows to sheet
            for i in range(0, total, BATCH_SIZE):
                batch = COMPANY_NAMES[i:i + BATCH_SIZE]
                logger.info(f"Updater: processing batch {i // BATCH_SIZE + 1} / {(total + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} companies)")
                processed = await update_batch(batch)  # returns list of (company, news_list)
                # For this batch, flatten rows and write to sheet immediately (incremental)
                batch_rows = []
                for comp, news_list in processed:
                    # ensure news sorted newest->oldest and trimmed to 5
                    rows = flatten_company_rows_for_write(comp, news_list, keep=5)
                    batch_rows.extend(rows)
                # write chunk for this batch
                if batch_rows and service and SHEET_ID:
                    try:
                        # chunk further if batch_rows too large, but usually batch small
                        written = 0
                        while written < len(batch_rows):
                            chunk = batch_rows[written:written + SHEET_CHUNK_SIZE]
                            write_rows_chunk(service, chunk, start_row=write_pointer)
                            written += len(chunk)
                            write_pointer += len(chunk)
                    except Exception as e:
                        logger.error(f"Error writing batch chunk to sheet: {e}")

                # occasional save to local cache file is handled by periodic saver
                await asyncio.sleep(random.uniform(0.5, 1.5))

            logger.info(f"Background update cycle finished. Cached companies: {len(NEWS_CACHE)}")

            # After batches, add 'All' top items block and sectors block at the end
            if service and SHEET_ID:
                try:
                    # Build 'All' top N (e.g., 50) rows and write them labelled company='All'
                    all_items = build_all_section(limit=60)
                    all_rows = []
                    for it in all_items:
                        all_rows.append([
                            "All",
                            it.get("title", ""),
                            it.get("link", ""),
                            it.get("pubDate", ""),
                            it.get("sentiment", detect_sentiment(it.get("title", "") + " " + it.get("description", "")))
                        ])
                    if all_rows:
                        write_rows_chunk(service, all_rows, start_row=write_pointer)
                        write_pointer += len(all_rows)

                    # Build sectors: for each sector pick top item and write as company='SECTOR:<NAME>'
                    sector_rows = []
                    for sk in SECTOR_KEYWORDS.keys():
                        top = build_sector_top_item(sk)
                        if top:
                            n = top["item"]
                            sector_rows.append([
                                f"SECTOR:{sk}",
                                n.get("title", ""),
                                n.get("link", ""),
                                n.get("pubDate", ""),
                                n.get("sentiment", detect_sentiment(n.get("title", "") + " " + n.get("description", "")))
                            ])
                    if sector_rows:
                        write_rows_chunk(service, sector_rows, start_row=write_pointer)
                        write_pointer += len(sector_rows)

                    logger.info("Wrote 'All' and sector summary rows to sheet.")
                except Exception as e:
                    logger.error(f"Failed to write All/Sector rows: {e}")

            # Immediately start next cycle (no long sleep). small 1s pause to avoid busy-loop
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Background updater crashed: {e}")
            await asyncio.sleep(5)

# -----------------------------
# API endpoints (unchanged for most, but sheet writes use new format)
# -----------------------------
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
            n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
    return {"company": company_name, "news": news}

@api_router.get("/news/all")
async def get_all_news(
    days: Optional[int] = Query(None, description="Optional filter: last N days"),
    only_impact: Optional[bool] = Query(False, description="If true, only return high impact items"),
    include_indexes: Optional[bool] = Query(False, description="If true, return grouped sections (indexes/largecap/general)"),
    same_day: Optional[bool] = Query(False, description="If true, prefer same-day (today) news; fallback to yesterday if none")
):
    # keep original behaviour using build_all_section
    if not same_day:
        items = build_all_section(limit=150, days=days, only_impact=only_impact)
        for n in items:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
        if include_indexes:
            indexes = [i for i in items if any(k in (i.get("title","")+" "+i.get("description","")).lower() for k in INDEX_NEWS_KEYS)]
            largecap = build_largecap_section(limit=60, days=days)
            general = build_general_section(limit=150, days=days)
            for arr in (indexes, largecap, general):
                for n in arr:
                    if "sentiment" not in n:
                        n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
            return {
                "sections": {
                    "indexes": indexes,
                    "largecap": largecap,
                    "general": general
                },
                "count": len(indexes) + len(largecap) + len(general)
            }
        return {"news": items, "count": len(items)}

    # same_day path (unchanged logic)
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
                    n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
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
                        n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
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
                n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
        return {"news": flat, "count": len(flat)}

    indexes_y = build_index_section(limit=60, days=yesterday_days)
    largecap_y = build_largecap_section(limit=60, days=yesterday_days)
    general_y = build_general_section(limit=150, days=yesterday_days)

    if not (indexes_y or largecap_y or general_y):
        fallback_items = build_all_section(limit=150, days=None, only_impact=only_impact)
        for n in fallback_items:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
        if include_indexes:
            idx = build_index_section(limit=60, days=None)
            lc = build_largecap_section(limit=60, days=None)
            gen = build_general_section(limit=150, days=None)
            for arr in (idx, lc, gen):
                for n in arr:
                    if "sentiment" not in n:
                        n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
            return {"sections": {"indexes": idx, "largecap": lc, "general": gen}, "count": len(idx) + len(lc) + len(gen)}
        return {"news": fallback_items, "count": len(fallback_items)}

    if include_indexes:
        for arr in (indexes_y, largecap_y, general_y):
            for n in arr:
                if "sentiment" not in n:
                    n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
        return {"sections": {"indexes": indexes_y, "largecap": largecap_y, "general": general_y}, "count": len(indexes_y) + len(largecap_y) + len(general_y)}

    flat_y = remove_duplicates((indexes_y or []) + (largecap_y or []) + (general_y or []))[:150]
    for n in flat_y:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
    return {"news": flat_y, "count": len(flat_y)}

@api_router.get("/news/results")
async def get_results_news(days: Optional[int] = Query(None)):
    items = build_results_section(limit=200, days=days)
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
    return {"news": items, "count": len(items)}

@api_router.get("/news/sector/{sector_name}")
async def get_sector_news(sector_name: str, days: Optional[int] = Query(None)):
    s = sector_name.upper()
    if s == "PENNY":
        items = build_penny_section(days=days)
    elif s == "LARGECAP" or s == "LARGE CAP":
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
            n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
    return {"news": items, "count": len(items)}

@api_router.get("/status")
async def get_status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE),
        "cache_duration_minutes": CACHE_DURATION / 60,
        "sheet_id": bool(SHEET_ID),
        "sheet_tab": SHEET_TAB_NAME
    }

@api_router.get("/ping")
async def ping():
    return {"status": "alive", "time": time.time()}

@app.get("/")
async def root():
    html = "<html><body><h2>Stock News Backend</h2><p>API available at <code>/api/</code></p></body></html>"
    return Response(content=html, media_type="text/html")

# -----------------------------
# Debug endpoints (manual testing)
# -----------------------------
@api_router.get("/debug/push_sheet_test")
async def debug_push_sheet_test():
    try:
        for company, data in NEWS_CACHE.items():
            items = data.get("news", [])
            if items:
                n = items[0]
                row = [[company,
                        n.get("title", ""),
                        n.get("link", ""),
                        n.get("pubDate", ""),
                        n.get("sentiment", detect_sentiment(n.get("title", "") + " " + n.get("description", "")))]]
                service = get_sheets_service()
                if not service or not SHEET_ID:
                    raise HTTPException(status_code=500, detail="Sheets service or SHEET_ID not configured.")
                try:
                    clear_sheet(service)
                except Exception:
                    pass
                try:
                    write_rows_chunk(service, row, start_row=2)
                    return {"status": "ok", "written_rows": 1, "company": company}
                except Exception as e:
                    logger.error(f"Test write failed: {e}")
                    raise HTTPException(status_code=500, detail=f"Test write failed: {e}")
        return {"status": "no_data", "message": "No cached company with news found yet."}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"debug push error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/debug/run_once")
async def debug_run_once(limit: Optional[int] = Query(10, description="Number of companies to update (first N companies)")):
    try:
        if not COMPANY_NAMES:
            raise HTTPException(status_code=400, detail="No companies loaded.")
        to_process = COMPANY_NAMES[:max(1, int(limit))]
        processed = await update_batch(to_process)
        # write only the processed rows
        service = get_sheets_service()
        if service and SHEET_ID:
            try:
                clear_sheet(service)
            except Exception:
                pass
            rows = []
            ptr = 2
            for comp, news_list in processed:
                r = flatten_company_rows_for_write(comp, news_list, keep=5)
                if r:
                    write_rows_chunk(service, r, start_row=ptr)
                    ptr += len(r)
                    rows.extend(r)
        return {"status": "ok", "processed_companies": len(to_process), "written_rows": len(rows) if 'rows' in locals() else 0}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"debug run_once error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.get("/debug/write_all")
async def debug_write_all():
    try:
        # Fully flush NEWS_CACHE into sheet (useful after cycle)
        service = get_sheets_service()
        if not service or not SHEET_ID:
            raise HTTPException(status_code=500, detail="Sheets service or SHEET_ID not configured.")
        clear_sheet(service)
        ptr = 2
        total_rows = 0
        for company, data in NEWS_CACHE.items():
            rows = flatten_company_rows_for_write(company, data.get("news", []), keep=5)
            if rows:
                # may chunk but these per-company rows are small
                write_rows_chunk(service, rows, start_row=ptr)
                ptr += len(rows)
                total_rows += len(rows)
        # add All + sector rows
        all_items = build_all_section(limit=60)
        all_rows = []
        for it in all_items:
            all_rows.append([
                "All",
                it.get("title", ""),
                it.get("link", ""),
                it.get("pubDate", ""),
                it.get("sentiment", detect_sentiment(it.get("title", "") + " " + it.get("description", "")))
            ])
        if all_rows:
            write_rows_chunk(service, all_rows, start_row=ptr)
            total_rows += len(all_rows)
            ptr += len(all_rows)
        sector_rows = []
        for sk in SECTOR_KEYWORDS.keys():
            top = build_sector_top_item(sk)
            if top:
                n = top["item"]
                sector_rows.append([
                    f"SECTOR:{sk}",
                    n.get("title", ""),
                    n.get("link", ""),
                    n.get("pubDate", ""),
                    n.get("sentiment", detect_sentiment(n.get("title", "") + " " + n.get("description", "")))
                ])
        if sector_rows:
            write_rows_chunk(service, sector_rows, start_row=ptr)
            total_rows += len(sector_rows)
        return {"status": "ok", "total_rows": total_rows}
    except Exception as e:
        logger.error(f"debug write_all error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

app.include_router(api_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# -----------------------------
# Startup & Shutdown Events
# -----------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("Server starting: loading companies and cache...")
    load_company_names()
    load_cache_from_file()
    # start background tasks
    asyncio.create_task(background_news_updater())
    asyncio.create_task(save_cache_periodically())
    logger.info("Startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Server shutting down: saving cache...")
    try:
        normalize_news_cache()
        with open(CACHE_FILE, "w") as f:
            json.dump(NEWS_CACHE, f, ensure_ascii=False, indent=2, default=str)
        logger.info("Shutdown saved cache")
    except Exception as e:
        logger.error(f"Error saving cache on shutdown: {e}")
    logger.info("Shutdown complete")
