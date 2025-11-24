# server.py
import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote
from email.utils import parsedate_to_datetime

import feedparser
import PyPDF2
from fastapi import FastAPI, APIRouter, Query, Response
from starlette.middleware.cors import CORSMiddleware

ROOT_DIR = Path(__file__).parent
COMPANY_PDF = Path("/mnt/data/combined_companies.pdf")   # <- uses your uploaded PDF
CACHE_FILE = ROOT_DIR / "news_cache.json"
CACHE_DURATION = 15 * 60         # seconds between full cycles
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stock-news-backend")

app = FastAPI()
api_router = APIRouter(prefix="/api")

# ---- Runtime state ----
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}  # company -> {"news":[...], "timestamp": t}

# Index keywords (case insensitive)
INDEX_NEWS_KEYS = ["nifty", "sensex", "banknifty", "nifty bank", "index", "market", "benchmark"]

# List of top largecaps you want to include in the "Largecap" section.
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
    "HCL Technologies Limited",
]

GOOD_KEYWORDS = ["profit", "record", "growth", "surge", "beats", "upgrade", "wins", "strong", "rise", "positive"]
BAD_KEYWORDS = ["loss", "fraud", "scam", "crash", "decline", "penalty", "investigation", "downgrade", "fall"]

# ---- small utilities ----
def clean_html(text: Optional[str]) -> str:
    if not text:
        return ""
    txt = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", txt).strip()

def detect_sentiment(text: str) -> str:
    t = (text or "").lower()
    for k in GOOD_KEYWORDS:
        if k in t:
            return "good"
    for k in BAD_KEYWORDS:
        if k in t:
            return "bad"
    return "neutral"

def remove_duplicates(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for it in items:
        key = (it.get("title", "").strip().lower(), it.get("link", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def parse_date(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None

def within_days(item: Dict, days: Optional[int]) -> bool:
    if not days:
        return True
    pub = item.get("pubDate", "")
    dt = parse_date(pub)
    if not dt:
        return False
    return (datetime.now(timezone.utc) - dt).days <= days

# ---- load companies from PDF ----
def load_company_names():
    global COMPANY_NAMES
    if not COMPANY_PDF.exists():
        logger.warning(f"Company PDF not found at {COMPANY_PDF}, COMPANY_NAMES empty.")
        COMPANY_NAMES = []
        return
    try:
        with open(COMPANY_PDF, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            text = "".join((page.extract_text() or "") for page in reader.pages)
        names = []
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            # Heuristic: include common suffixes
            if any(s in ln for s in ("Limited", "Ltd", "LTD", "ETF", "Bank", "Industries", "Industrials")):
                names.append(ln)
        # unique preserve order
        seen = set()
        COMPANY_NAMES = [n for n in names if not (n in seen or seen.add(n))]
        logger.info(f"Loaded {len(COMPANY_NAMES)} companies from PDF")
    except Exception as e:
        logger.exception("Failed to read PDF for company list: %s", e)
        COMPANY_NAMES = []

# ---- simple persistent cache load/save ----
def load_cache_from_file():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    NEWS_CACHE.update(data)
            logger.info("Loaded news cache from file (%d companies cached)", len(NEWS_CACHE))
        except Exception as e:
            logger.error("Could not load cache file: %s", e)

async def save_cache_periodically():
    while True:
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(NEWS_CACHE, f, ensure_ascii=False, indent=2, default=str)
            logger.debug("Saved cache file")
        except Exception as e:
            logger.error("Error saving cache: %s", e)
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)

# ---- RSS fetching (Google News RSS) ----
async def fetch_company_news(company_name: str, max_items=6) -> List[Dict]:
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        feed = await asyncio.to_thread(feedparser.parse, url)
        items = []
        for entry in feed.entries[:max_items]:
            title = clean_html(entry.get("title", "") or "")
            summary = clean_html(entry.get("summary", "") or entry.get("description", "") or "")
            link = entry.get("link") or entry.get("id") or ""
            pub = entry.get("published") or entry.get("updated") or ""
            combined = (title + " " + summary).strip()
            items.append({
                "title": title,
                "description": summary,
                "link": link,
                "pubDate": pub,
                "raw_text": combined
            })
        items = remove_duplicates(items)
        # normalize pubDate to iso where possible
        for it in items:
            dt = parse_date(it.get("pubDate", "") or "")
            if dt:
                it["pubDate"] = dt.isoformat()
        return items[:5]
    except Exception as e:
        logger.exception("fetch_company_news error for %s: %s", company_name, e)
        return []

# ---- update / background ----
async def update_one_company(company: str):
    news = await fetch_company_news(company)
    for n in news:
        n["sentiment"] = detect_sentiment(n.get("title", "") + " " + n.get("description", ""))
    NEWS_CACHE[company] = {"news": news, "timestamp": time.time()}

async def update_batch(companies: List[str]):
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    async def worker(c):
        async with sem:
            await update_one_company(c)
            await asyncio.sleep(random.uniform(0.2, 0.6))
    await asyncio.gather(*(worker(c) for c in companies), return_exceptions=True)

async def background_updater():
    logger.info("Background updater started")
    while True:
        try:
            if not COMPANY_NAMES:
                await asyncio.sleep(5)
                continue
            total = len(COMPANY_NAMES)
            for i in range(0, total, BATCH_SIZE):
                await update_batch(COMPANY_NAMES[i:i+BATCH_SIZE])
            logger.info("Cycle finished: cached %d companies", len(NEWS_CACHE))
            await asyncio.sleep(CACHE_DURATION)
        except Exception as e:
            logger.exception("Background updater error: %s", e)
            await asyncio.sleep(30)

# ---- builders for sections ----
def build_index_section(limit=50, days: Optional[int] = None):
    """Search cached items for index/market-wide keywords (nifty / sensex / banknifty)."""
    out = []
    for comp, data in NEWS_CACHE.items():
        for n in data.get("news", []):
            txt = (n.get("title", "") + " " + n.get("description", "")).lower()
            if any(k in txt for k in INDEX_NEWS_KEYS):
                if days and not within_days(n, days):
                    continue
                item = n.copy(); item["company"] = comp
                out.append(item)
    out = remove_duplicates(out)
    out.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    return out[:limit]

def build_largecap_section(limit=60, days: Optional[int] = None):
    out = []
    for top in TOP_STOCKS:
        for n in NEWS_CACHE.get(top, {}).get("news", []):
            if days and not within_days(n, days):
                continue
            item = n.copy(); item["company"] = top
            out.append(item)
            break
    out = remove_duplicates(out)
    out.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    return out[:limit]

def build_general_section(limit=150, days: Optional[int] = None):
    all_items = []
    for comp, data in NEWS_CACHE.items():
        for n in data.get("news", []):
            if days and not within_days(n, days):
                continue
            it = n.copy(); it["company"] = comp
            all_items.append(it)
    all_items = remove_duplicates(all_items)
    all_items.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    return all_items[:limit]

# ---- API endpoints ----
@api_router.get("/status")
async def status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE),
        "cache_duration_seconds": CACHE_DURATION
    }

@api_router.get("/companies/search")
async def search_companies(q: str = Query("", description="search text")):
    if not q:
        return []
    ql = q.lower()
    matches = [n for n in COMPANY_NAMES if ql in n.lower()]
    return matches[:50]

@api_router.get("/news/indexes")
async def indexes_news(days: Optional[int] = Query(None, description="last N days")):
    idx = build_index_section(days=days)
    for n in idx:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
    return {"news": idx, "count": len(idx)}

@api_router.get("/news/largecap")
async def largecap_news(days: Optional[int] = Query(None)):
    arr = build_largecap_section(days=days)
    for n in arr:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
    return {"news": arr, "count": len(arr)}

@api_router.get("/news/general")
async def general_news(days: Optional[int] = Query(None)):
    arr = build_general_section(days=days)
    for n in arr:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
    return {"news": arr, "count": len(arr)}

@api_router.post("/admin/refresh-company")
async def refresh_company(name: str = Query(..., description="exact company name")):
    """Trigger fetch for a single company (useful for testing)."""
    await update_one_company(name)
    return {"ok": True, "company": name, "cached_items": len(NEWS_CACHE.get(name, {}).get("news", []))}

@api_router.get("/ping")
async def ping():
    return {"status": "alive", "time": time.time()}

# ---- wiring & startup ----
app.include_router(api_router)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup_event():
    logger.info("Starting server: loading companies & cache")
    load_company_names()          # loads COMPANY_NAMES from PDF path above
    load_cache_from_file()
    # start background jobs
    asyncio.create_task(background_updater())
    asyncio.create_task(save_cache_periodically())
    logger.info("Startup complete")

@app.get("/")
async def root():
    return Response(content="<h2>Stock News Backend</h2><p>API at /api/</p>", media_type="text/html")
