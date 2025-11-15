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

from fastapi import FastAPI, APIRouter, Query
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote
from typing import Dict, List

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------
# Config
# -----------------------------
CACHE_FILE = ROOT_DIR / "news_cache.json"
COMPANY_PDF = ROOT_DIR / "company_list.pdf"
CACHE_DURATION = 15 * 60  # minutes*60
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60

# -----------------------------
# Global state
# -----------------------------
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}  # company -> {"news": [...], "timestamp": t}
INDEX_NEWS_KEYS = ["nifty", "sensex", "banknifty", "nifty bank", "index"]

# -----------------------------
# Top / Nifty lists (editable)
# -----------------------------
# This list should contain the major Nifty/Sensex stocks you want in ALL section.
TOP_STOCKS = [
    "Reliance Industries Limited", "Tata Consultancy Services Limited",
    "HDFC Bank Limited", "ICICI Bank Limited", "Infosys Limited",
    "Hindustan Unilever Limited", "State Bank of India", "Larsen & Toubro Limited",
    "Bharti Airtel Limited", "ITC Limited", "Tata Motors Limited",
    "Kotak Mahindra Bank Limited", "Axis Bank Limited", "Maruti Suzuki India Limited",
    "Bajaj Finance Limited", "Mahindra & Mahindra Limited", "Wipro Limited",
    "Power Grid Corporation of India Limited", "Asian Paints Limited", "HCL Technologies Limited"
]

# -----------------------------
# Penny stocks sample list (you can expand this file or load from CSV)
# -----------------------------
PENNY_STOCKS = [
    # Example small list; expand or load from file for full coverage
    "Tilaknagar Industries Limited", "3i Infotech Limited", "XYZ Penny Ltd"
]

# -----------------------------
# Sectors mapping: simple keyword-based fallback
# -----------------------------
SECTOR_KEYWORDS = {
    "FMCG": ["fmcg", "food", "beverage", "consumer goods", "packaged", "retail"],
    "HEALTH": ["pharma", "hospital", "healthcare", "vaccine", "biotech", "drug"],
    "IT": ["software", "it", "technology", "digital", "tcs", "infosys", "wipro"],
    "BANKING": ["bank", "banking", "hdfc", "icici", "sbi", "kotak", "axis"],
    "AUTO": ["auto", "automobile", "vehicle", "motors", "maruti", "tata motors"],
    "METALS": ["steel", "metal", "mining", "ore"],
    "ENERGY": ["oil", "energy", "gas", "petro", "bpcl", "hpcl", "oil and gas"],
    "PSU": ["psu", "public sector", "state"],
    "TELECOM": ["telecom", "airtel", "vodafone", "jio", "telecom"],
    "MIDCAP": ["midcap"],
    "SMALLCAP": ["smallcap"],
    "FINANCE": ["finance", "nbfc", "lending", "bajaj finance"],
    "INDEX": ["index", "nifty", "sensex", "bank nifty"]
}

# -----------------------------
# Sentiment keywords (expanded)
# -----------------------------
GOOD_KEYWORDS = [
    "profit", "record", "growth", "surge", "beats", "beat", "upgrade", "acquisition",
    "wins", "expansion", "increase", "strong", "soars", "rise", "raised", "positive"
]

BAD_KEYWORDS = [
    "loss", "fraud", "scam", "crash", "down", "decline", "penalty", "investigation",
    "downgrade", "miss", "misses", "fall", "weak", "slump", "reuters", "lawsuit"
]

IMPACT_KEYWORDS = GOOD_KEYWORDS + BAD_KEYWORDS + [
    "merger", "deal", "insider", "earnings", "quarterly", "results", "scam", "FDI", "investment", "SEBI", "revenue"
]

# -----------------------------
# Utils
# -----------------------------
def clean_html(text: str) -> str:
    if not text:
        return ""
    # remove html tags
    text = re.sub(r"<[^>]+>", "", text)
    # replace multiple spaces/newlines with single space
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

def is_high_impact(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in IMPACT_KEYWORDS)

def remove_duplicates(news_list: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for n in news_list:
        key = (n.get("title","").strip().lower(), n.get("link","").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out

# -----------------------------
# Load companies from PDF
# -----------------------------
def load_company_names():
    global COMPANY_NAMES
    if not COMPANY_PDF.exists():
        logger.error(f"Company PDF not found: {COMPANY_PDF}")
        return
    try:
        with open(COMPANY_PDF, "rb") as f:
            pdf = PyPDF2.PdfReader(f)
            text = "".join((page.extract_text() or "") for page in pdf.pages)
        companies = []
        for line in text.split("\n"):
            line = line.strip()
            if line and ("Limited" in line or "Ltd" in line or "ETF" in line):
                companies.append(line)
        seen = set()
        COMPANY_NAMES = [x for x in companies if not (x in seen or seen.add(x))]
        logger.info(f"Loaded {len(COMPANY_NAMES)} companies from PDF")
    except Exception as e:
        logger.error(f"Error reading company PDF: {e}")

# -----------------------------
# Persistent cache load/save
# -----------------------------
def load_cache_from_file():
    global NEWS_CACHE
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    NEWS_CACHE.update(data)
            logger.info(f"Loaded {len(NEWS_CACHE)} companies from cache file")
        except Exception as e:
            logger.error(f"Error loading cache file: {e}")

async def save_cache_periodically():
    while True:
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(NEWS_CACHE, f)
            logger.info(f"Saved cache to file ({len(NEWS_CACHE)} companies)")
        except Exception as e:
            logger.error(f"Error saving cache file: {e}")
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)

# -----------------------------
# RSS fetching
# -----------------------------
async def fetch_company_news(company_name: str) -> List[Dict]:
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        feed = await asyncio.to_thread(feedparser.parse, url)
        news_items = []
        for entry in feed.entries[:8]:  # fetch up to 8 then we trim later
            title = clean_html(entry.get("title", "") or "")
            summary = clean_html(entry.get("summary", "") or entry.get("description", "") or "")
            link = entry.get("link", "") or entry.get("id","")
            pubDate = entry.get("published", "")
            news_items.append({
                "title": title,
                "description": summary,
                "link": link,
                "pubDate": pubDate
            })
        # dedupe locally
        news_items = remove_duplicates(news_items)
        # keep top 5 concise
        return news_items[:5]
    except Exception as e:
        logger.error(f"Error fetching news for {company_name}: {e}")
        return []

# -----------------------------
# Background updater helpers
# -----------------------------
async def update_one_company(company: str):
    """
    Fetch news for one company.
    Replace cached news only if fetch returns non-empty list.
    If fetch fails or returns empty, keep old cache (do not delete).
    """
    try:
        news = await fetch_company_news(company)
        # enrich sentiment
        for n in news:
            txt = (n.get("title","") + " " + n.get("description","")).strip()
            n["sentiment"] = detect_sentiment(txt)
        if news:
            NEWS_CACHE[company] = {"news": news, "timestamp": time.time()}
        else:
            # if no news but company not in cache -> store empty (to mark fetched once)
            if company not in NEWS_CACHE:
                NEWS_CACHE[company] = {"news": [], "timestamp": time.time()}
        return True
    except Exception as e:
        logger.error(f"update_one_company error for {company}: {e}")
        return False

async def update_batch(companies: List[str], semaphore_limit: int = SEMAPHORE_LIMIT):
    sem = asyncio.Semaphore(semaphore_limit)
    async def _worker(c):
        async with sem:
            await update_one_company(c)
    tasks = [ _worker(c) for c in companies ]
    await asyncio.gather(*tasks, return_exceptions=True)

# -----------------------------
# Background updater loop
# -----------------------------
async def background_news_updater():
    logger.info("Background updater started")
    while True:
        try:
            total = len(COMPANY_NAMES)
            if total == 0:
                await asyncio.sleep(10)
                continue
            for i in range(0, total, BATCH_SIZE):
                batch = COMPANY_NAMES[i:i+BATCH_SIZE]
                logger.info(f"Updater: processing batch {i//BATCH_SIZE + 1} / { (total+BATCH_SIZE-1)//BATCH_SIZE } ({len(batch)} companies)")
                await update_batch(batch)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            logger.info(f"Background update cycle finished. Cached companies: {len(NEWS_CACHE)}")
            # wait until next full cycle
            await asyncio.sleep(CACHE_DURATION)
        except Exception as e:
            logger.error(f"Background updater crashed: {e}")
            await asyncio.sleep(60)

# -----------------------------
# Aggregation & filters for API
# -----------------------------
def build_all_section(limit=150):
    """
    Build the premium ALL section:
    - Focus on TOP_STOCKS and index news.
    - One top/high-impact item per company.
    - Ensure variety and dedupe.
    """
    results = []
    added_companies = set()

    # 1) index news first (if present in cache)
    for company, cache in NEWS_CACHE.items():
        for item in cache.get("news", []):
            txt = (item.get("title","") + " " + item.get("description","")).lower()
            if any(k in txt for k in INDEX_NEWS_KEYS):
                x = item.copy(); x["company"] = company
                if x.get("title") and (company not in added_companies):
                    results.append(x)
                    added_companies.add(company)
                    break

    # 2) top stocks - 1 item each, highest impact first
    for top in TOP_STOCKS:
        if top in NEWS_CACHE and top not in added_companies:
            # pick most impactful item (priority: impact, then latest)
            candidates = NEWS_CACHE[top].get("news", [])
            candidates = [c for c in candidates if c.get("title")]
            if not candidates:
                continue
            # sort by impact presence and then pubDate (best-effort)
            candidates.sort(key=lambda it: (not is_high_impact(it.get("title","") + " " + it.get("description","")), it.get("pubDate","")), reverse=False)
            chosen = candidates[0]
            x = chosen.copy(); x["company"] = top
            if x["title"]:
                results.append(x)
                added_companies.add(top)

    # 3) fill remaining with other impactful companies (max 1 per company)
    for company, cache in NEWS_CACHE.items():
        if company in added_companies:
            continue
        for item in cache.get("news", []):
            if is_high_impact(item.get("title","") + " " + item.get("description","")):
                x = item.copy(); x["company"] = company
                results.append(x)
                added_companies.add(company)
                break
        if len(results) >= limit:
            break

    # 4) dedupe & trim
    results = remove_duplicates(results)
    return results[:limit]

def build_sector_section(keywords: List[str], limit=150):
    items = []
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title","") + " " + n.get("description","")).lower()
            if any(k.lower() in txt for k in keywords):
                x = n.copy(); x["company"] = company
                items.append(x)
    items = remove_duplicates(items)
    return items[:limit]

def build_penny_section(limit=150):
    items = []
    for p in PENNY_STOCKS:
        cache = NEWS_CACHE.get(p, {})
        for n in cache.get("news", []):
            x = n.copy(); x["company"] = p
            items.append(x)
    items = remove_duplicates(items)
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
    # also fuzzy inclusion
    if not matches:
        matches = [name for name in COMPANY_NAMES if ql in name.lower()]
    return matches[:50]

@api_router.get("/news/company/{company_name}")
async def get_news_by_company(company_name: str):
    # returns cached news only (instant). If not present return empty list.
    news = NEWS_CACHE.get(company_name, {}).get("news", [])
    # ensure sentiment exists
    for n in news:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment((n.get("title","") + " " + n.get("description","")))
    return {"company": company_name, "news": news}

@api_router.get("/news/all")
async def get_all_news():
    items = build_all_section(limit=150)
    # ensure sentiment exists
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment((n.get("title","") + " " + n.get("description","")))
    return {"news": items, "count": len(items)}

@api_router.get("/news/sector/{sector_name}")
async def get_sector_news(sector_name: str):
    name = sector_name.upper()
    if name == "PENNY":
        items = build_penny_section()
    elif name == "INDEX":
        items = build_sector_section(INDEX_NEWS_KEYS)
    else:
        keywords = SECTOR_KEYWORDS.get(name, [name])
        items = build_sector_section(keywords)
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment((n.get("title","") + " " + n.get("description","")))
    return {"news": items, "count": len(items)}

@api_router.get("/status")
async def get_status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE),
        "cache_duration_minutes": CACHE_DURATION / 60
    }

# -----------------------------
# App wiring
# -----------------------------
app.include_router(api_router)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# -----------------------------
# Startup & shutdown tasks
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
        with open(CACHE_FILE, "w") as f:
            json.dump(NEWS_CACHE, f)
    except Exception as e:
        logger.error(f"Error saving cache on shutdown: {e}")
    logger.info("Shutdown complete")
