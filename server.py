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

from fastapi import FastAPI, APIRouter, Query, Response
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote
from typing import Dict, List, Optional
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

# Google Sheets imports
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
CACHE_DURATION = 90 * 60  # 1.5 hours (changed from 15 minutes)
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60

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
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")  # must be set in env
SHEET_RANGE = "Sheet1!A2"  # start writing after header row

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


def clear_sheet():
    service = get_sheets_service()
    if not service or not SHEET_ID:
        logger.warning("Skipping clear_sheet: service or SHEET_ID missing.")
        return
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID,
            range="Sheet1!A2:F200000",
            body={}
        ).execute()
    except Exception as e:
        logger.error(f"Error clearing sheet: {e}")


def write_news_to_sheet(all_news_rows):
    service = get_sheets_service()
    if not service or not SHEET_ID:
        logger.warning("Skipping write_news_to_sheet: service or SHEET_ID missing.")
        return
    if not all_news_rows:
        logger.info("No rows to write to Google Sheet.")
        return
    try:
        body = {"values": all_news_rows}
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=SHEET_RANGE,
            valueInputOption="RAW",
            body=body
        ).execute()
    except Exception as e:
        logger.error(f"Error writing to sheet: {e}")

# -----------------------------
# Helpers to flatten NEWS_CACHE for sheet
# -----------------------------
def flatten_all_news():
    rows = []
    for company, data in NEWS_CACHE.items():
        for item in data.get("news", []):
            rows.append([
                company,
                item.get("title", ""),
                item.get("description", ""),
                item.get("link", ""),
                item.get("pubDate", ""),
                item.get("sentiment", "")
            ])
    return rows

# -----------------------------
# Load companies from PDF
# -----------------------------
def load_company_names():
    global COMPANY_NAMES
    # Primary: configured COMPANY_PDF (company_list.pdf in project root)
    # Fallback: uploaded combined_companies.pdf (path provided)
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
            # Heuristic: lines that look like company names
            if line and ("Limited" in line or "Ltd" in line or "ETF" in line or "Corporation" in line or "Industries" in line):
                companies.append(line)
        # dedupe preserving order
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
    """
    Query news.google.com RSS for "<company_name> stock"
    Returns up to 5 cleaned items with title, description, link, pubDate, raw_text
    """
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        # feedparser is sync; run in thread to avoid blocking event loop
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
                "description": summary,
                "link": link,
                "pubDate": pubDate,
                "raw_text": text_combined
            })
        news_items = remove_duplicates(news_items)
        # normalize pubDate to iso if possible
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
            n["mentioned_companies"] = []  # placeholder, can be populated later
        timestamp = time.time()
        if news:
            NEWS_CACHE[company] = {"news": news, "timestamp": timestamp}
        elif company not in NEWS_CACHE:
            # ensure company exists in cache even with empty list to avoid missing keys
            NEWS_CACHE[company] = {"news": [], "timestamp": timestamp}
    except Exception as e:
        logger.error(f"update_one_company error for {company}: {e}")

# -----------------------------
# Concurrency batch
# -----------------------------
async def update_batch(companies: List[str]):
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async def worker(c):
        async with sem:
            await update_one_company(c)

    await asyncio.gather(*[worker(c) for c in companies], return_exceptions=True)

# -----------------------------
# Background updater
# -----------------------------
async def background_news_updater():
    logger.info("Background updater started")
    while True:
        try:
            total = len(COMPANY_NAMES)
            if total == 0:
                logger.info("No companies loaded yet, sleeping before retrying...")
                await asyncio.sleep(10)
                continue

            for i in range(0, total, BATCH_SIZE):
                batch = COMPANY_NAMES[i:i + BATCH_SIZE]
                logger.info(
                    f"Updater: processing batch {i // BATCH_SIZE + 1} / {(total + BATCH_SIZE - 1) // BATCH_SIZE} ({len(batch)} companies)"
                )
                await update_batch(batch)
                # tiny random sleep to avoid hammering remote
                await asyncio.sleep(random.uniform(0.5, 1.5))

            logger.info(f"Background update cycle finished. Cached companies: {len(NEWS_CACHE)}")

            # -----------------------------
            # Push flattened news to Google Sheet
            # -----------------------------
            try:
                logger.info("Flattening news for Google Sheet...")
                rows = flatten_all_news()

                logger.info("Clearing old rows in Google Sheet...")
                clear_sheet()

                logger.info(f"Writing {len(rows)} rows to Google Sheet...")
                write_news_to_sheet(rows)

                logger.info("Google Sheet updated successfully!")
            except Exception as e:
                logger.error(f"Failed to update Google Sheet: {e}")

            await asyncio.sleep(CACHE_DURATION)
        except Exception as e:
            logger.error(f"Background updater crashed: {e}")
            await asyncio.sleep(60)

# -----------------------------
# Helpers for filtering/sorting
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
        # treat unknown parse as valid recent
        return True
    now = datetime.now(timezone.utc)
    delta = now - dt
    return delta.days <= days

# -----------------------------
# Builders for API sections
# -----------------------------
def build_index_section(limit=50, days: Optional[int] = None):
    """Collect index-related items (nifty / sensex / banknifty keywords)"""
    results = []
    added = set()
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title", "") + " " + n.get("description", "")).lower()
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
    except Exception:
        pass
    return results[:limit]


def build_largecap_section(limit=60, days: Optional[int] = None):
    """Collect news for TOP_STOCKS (large / famous stocks)."""
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
    except Exception:
        pass
    return items[:limit]


def build_general_section(limit=150, days: Optional[int] = None):
    """General market news (fallback — returns recent items across cache)."""
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
    except Exception:
        pass
    return all_items[:limit]


def build_all_section(limit=150, days: Optional[int] = None, only_impact=False):
    """
    ALL section: index/news first, then TOP_STOCKS, then other impactful companies.
    If days is provided (int), only include items within that many days.
    If only_impact is True, prefer/only include high-impact items.
    """
    results = []
    added = set()

    # 1) index-like news first
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

    # 2) top stocks (prefer high impact)
    for top in TOP_STOCKS:
        if top in NEWS_CACHE and top not in added:
            candidates = NEWS_CACHE[top].get("news", [])
            # sort: high impact first then recent
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

    # 3) impactful others
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

    # 4) fill with recent / diverse if needed
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


def build_results_section(limit=150, days: Optional[int] = None):
    results = []
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            if days and not within_days(n, days):
                continue
            txt = (n.get("title", "") + " " + n.get("description", "")).lower()
            if any(k in txt for k in ["q1", "q2", "q3", "q4", "quarter", "quarterly", "annual", "yearly", "results", "earnings", "net profit", "revenue", "pat", "eps"]):
                x = n.copy()
                x["company"] = company
                results.append(x)
                break
        if len(results) >= limit:
            break
    try:
        results.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except Exception:
        pass
    return remove_duplicates(results)[:limit]


def build_sector_section(keywords: List[str], limit=150, days: Optional[int] = None):
    items = []
    keys = [k.lower() for k in keywords]
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            if days and not within_days(n, days):
                continue
            txt = (n.get("title", "") + " " + n.get("description", "")).lower()
            if any(k in txt for k in keys):
                x = n.copy()
                x["company"] = company
                items.append(x)
    items = remove_duplicates(items)
    try:
        items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except Exception:
        pass
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
    try:
        items.sort(key=lambda it: it.get("pubDate", ""), reverse=True)
    except Exception:
        pass
    return items[:limit]

# -----------------------------
# API endpoints
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
    """
    Standard flat list (backwards compatible)
    If same_day=True -> try to return only today's news (days=0). If no index items today, include largecap/midcap today.
    If nothing found for today, fallback to yesterday (days=1).
    """
    # Normal path when same_day is not requested
    if not same_day:
        items = build_all_section(limit=150, days=days, only_impact=only_impact)
        for n in items:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
        if include_indexes:
            indexes = build_index_section(limit=60, days=days)
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

    # -----------------------------
    # same_day=True path
    # -----------------------------
    # Try today (days=0)
    today_days = 0
    yesterday_days = 1

    indexes = build_index_section(limit=60, days=today_days)
    largecap = build_largecap_section(limit=60, days=today_days)
    general = build_general_section(limit=150, days=today_days)

    # If no index-style items today, attempt to include largecap/midcap today (per your request)
    if not indexes:
        # If we don't have index news today, still include largecap/midcap/general today if present
        combined_today = remove_duplicates((largecap or []) + (general or []))
        if combined_today:
            # build flat items (or grouped if client asked)
            flat_today = combined_today[:150]
            for n in flat_today:
                if "sentiment" not in n:
                    n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
            if include_indexes:
                # return grouped but indexes empty
                return {
                    "sections": {
                        "indexes": [],
                        "largecap": largecap,
                        "general": general
                    },
                    "count": len((largecap or [])) + len((general or []))
                }
            return {"news": flat_today, "count": len(flat_today)}

    # If indexes exist today (good), return sections today
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
        # flatten in index-first order
        flat = remove_duplicates(indexes + largecap + general)[:150]
        for n in flat:
            if "sentiment" not in n:
                n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
        return {"news": flat, "count": len(flat)}

    # If reached here there was nothing for today. Fallback to yesterday (days=1)
    indexes_y = build_index_section(limit=60, days=yesterday_days)
    largecap_y = build_largecap_section(limit=60, days=yesterday_days)
    general_y = build_general_section(limit=150, days=yesterday_days)

    # If still nothing, finally fall back to standard build_all_section without day filter
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

    # Return yesterday's content
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
        "cache_duration_minutes": CACHE_DURATION / 60
    }


@api_router.get("/ping")
async def ping():
    return {"status": "alive", "time": time.time()}


@app.get("/")
async def root():
    html = "<html><body><h2>Stock News Backend</h2><p>API available at <code>/api/</code></p></body></html>"
    return Response(content=html, media_type="text/html")


# -----------------------------
# App wiring
# -----------------------------
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
