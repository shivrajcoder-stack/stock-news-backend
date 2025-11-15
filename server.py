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
from typing import Dict, List, Optional
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("server")

# -----------------------------
# Config
# -----------------------------
CACHE_FILE = ROOT_DIR / "news_cache.json"
COMPANY_PDF = ROOT_DIR / "company_list.pdf"
CACHE_DURATION = 15 * 60  # seconds between cycles (15 minutes)
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60
RECENT_DAYS = 7  # only show items within last 7 days in public sections

# -----------------------------
# Global state
# -----------------------------
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}  # company -> {"news": [...], "timestamp": t}
SECTOR_CACHE: Dict[str, List[Dict]] = {}  # precomputed sector results for instant returns
INDEX_NEWS_KEYS = ["nifty", "sensex", "banknifty", "nifty bank", "index"]

# -----------------------------
# Top / Nifty lists
# -----------------------------
TOP_STOCKS = [
    "Reliance Industries Limited", "Tata Consultancy Services Limited",
    "HDFC Bank Limited", "ICICI Bank Limited", "Infosys Limited",
    "Hindustan Unilever Limited", "State Bank of India", "Larsen & Toubro Limited",
    "Bharti Airtel Limited", "ITC Limited", "Tata Motors Limited",
    "Kotak Mahindra Bank Limited", "Axis Bank Limited", "Maruti Suzuki India Limited",
    "Bajaj Finance Limited", "Mahindra & Mahindra Limited", "Wipro Limited",
    "Power Grid Corporation of India Limited", "Asian Paints Limited", "HCL Technologies Limited"
]

PENNY_STOCKS = [
    "Tilaknagar Industries Limited", "3i Infotech Limited", "XYZ Penny Ltd"
]

SECTOR_ORDER = [
    "ALL", "RESULTS", "PENNY", "LARGE CAP", "MIDCAP", "SMALLCAP",
    "FMCG", "IT", "BANKING", "AUTO", "ENERGY", "PSU", "TELECOM"
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
    "profit", "record", "growth", "surge", "beats", "upgrade", "wins", "strong",
    "rise", "positive", "acquisition", "expansion"
]

BAD_KEYWORDS = [
    "loss", "fraud", "scam", "crash", "decline", "penalty", "investigation",
    "downgrade", "fall", "weak", "slump", "lawsuit"
]

IMPACT_KEYWORDS = GOOD_KEYWORDS + BAD_KEYWORDS + ["earnings", "results", "investment", "SEBI", "revenue"]

# -----------------------------
# Utils & parsing helpers
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
        key = (n.get("title","").strip().lower(), n.get("link","").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(n)
    return out

def parse_pubdate(s: str) -> Optional[datetime]:
    """Try to parse pubDate from RSS entry using email.utils parsedate_to_datetime"""
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        # ensure timezone-aware -> convert to naive UTC-like for comparison (we'll use dt)
        if dt.tzinfo:
            dt = dt.astimezone(tz=None).replace(tzinfo=None)
        return dt
    except Exception:
        # fallback: try common ISO parse
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

def is_recent(pubdate_str: str, days: int = RECENT_DAYS) -> bool:
    dt = parse_pubdate(pubdate_str)
    if not dt:
        return False
    cutoff = datetime.utcnow() - timedelta(days=days)
    return dt >= cutoff

# -----------------------------
# Short summary & fact extractors (rule-based)
# -----------------------------
_re_money = re.compile(r"(?:₹|Rs\.?|INR)\s?[\d,]+(?:\.\d+)?", flags=re.I)
_re_percent = re.compile(r"\d{1,3}\.\d+%|\d{1,3}%")
_re_number = re.compile(r"\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b")

def extract_financial_facts(text: str) -> Dict:
    facts = {}
    if not text:
        return facts
    t = text
    monies = _re_money.findall(t)
    if monies:
        facts["monies"] = monies
    percents = _re_percent.findall(t)
    if percents:
        facts["percents"] = percents
    m = re.search(r"\b(EPS|earnings per share)\b[: ]*\s*([₹Rs\.]*\s*\d+[\d,\.]*)", t, flags=re.I)
    if m:
        facts["eps"] = m.group(2).strip()
    rev = re.search(r"(?:revenue|sales)[^0-9\n]{0,20}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?)", t, flags=re.I)
    if rev:
        facts["revenue"] = rev.group(1).strip()
    profit = re.search(r"(?:net profit|profit after tax|PAT|net income)[^0-9\n]{0,20}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?)", t, flags=re.I)
    if profit:
        facts["net_profit"] = profit.group(1).strip()
    div = re.search(r"(?:dividend|payout)[^0-9\n]{0,20}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?(?:\s*/\s*share|\s*per\s*share)?)", t, flags=re.I)
    if div:
        facts["dividend"] = div.group(1).strip()
    buy = re.search(r"(?:buyback|repurchase)[^0-9\n]{0,40}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?)", t, flags=re.I)
    if buy:
        facts["buyback_amount"] = buy.group(1).strip()
    guidance_phrases = re.search(r"(?:guidance|expects|outlook|forecast|target)\s*[:\-\sa-z0-9,()%]*", t, flags=re.I)
    if guidance_phrases:
        facts["guidance_snippet"] = guidance_phrases.group(0).strip()
    return facts

def is_results_news(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in ["q1", "q2", "q3", "q4", "quarter", "quarterly", "annual", "yearly", "results", "earnings", "net profit", "revenue", "pat", "eps"])

def generate_short_summary(item: Dict) -> str:
    title = item.get("title", "") or ""
    desc = item.get("description", "") or ""
    txt = (title + " " + desc).strip()
    facts = extract_financial_facts(txt)
    parts = []
    if "revenue" in facts:
        parts.append(f"Revenue: {facts['revenue']}")
    if "net_profit" in facts:
        parts.append(f"Net profit: {facts['net_profit']}")
    if "eps" in facts:
        parts.append(f"EPS: {facts['eps']}")
    if "dividend" in facts:
        parts.append(f"Dividend: {facts['dividend']}")
    if "buyback_amount" in facts:
        parts.append(f"Buyback: {facts['buyback_amount']}")
    if parts:
        return " • ".join(parts)
    if desc:
        sentences = re.split(r'(?<=[\.\?\!])\s+', desc)
        if sentences and sentences[0].strip():
            s = sentences[0].strip()
            if len(s) > 180:
                s = s[:177].rsplit(' ',1)[0] + "..."
            return s
    if title:
        t = title
        if len(t) > 160:
            t = t[:157].rsplit(' ',1)[0] + "..."
        return t
    return ""

# -----------------------------
# Load companies from PDF
# -----------------------------
def load_company_names():
    global COMPANY_NAMES
    if not COMPANY_PDF.exists():
        logger.error(f"Company PDF missing at {COMPANY_PDF}. COMPANY_NAMES will be empty.")
        return
    try:
        with open(COMPANY_PDF, "rb") as f:
            pdf = PyPDF2.PdfReader(f)
            text = "".join([(page.extract_text() or "") for page in pdf.pages])
        companies = []
        for line in text.split("\n"):
            line = line.strip()
            if line and ("Limited" in line or "Ltd" in line or "ETF" in line):
                companies.append(line)
        seen = set()
        COMPANY_NAMES = [c for c in companies if not (c in seen or seen.add(c))]
        logger.info(f"Loaded {len(COMPANY_NAMES)} companies from PDF")
    except Exception as e:
        logger.error(f"Error loading company PDF: {e}")

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
            logger.info(f"Loaded cache file: {len(NEWS_CACHE)} companies")
        except Exception as e:
            logger.error(f"Failed to load cache file: {e}")

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
            pubDate = entry.get("published", "") or entry.get("updated", "")
            text_combined = (title + " " + summary).strip()
            news_items.append({
                "title": title,
                "description": summary,
                "link": link,
                "pubDate": pubDate,
                "raw_text": text_combined
            })
        news_items = remove_duplicates(news_items)
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
            txt = (n.get("title","") + " " + n.get("description","")).strip()
            n["sentiment"] = detect_sentiment(txt)
            # only generate summary/facts here (cheap)
            n["summary"] = generate_short_summary(n)
            n["facts"] = extract_financial_facts(txt)
            # pubdate parsed
            n["_parsed_pubdate"] = parse_pubdate(n.get("pubDate","") or "")
        # only replace cache if we fetched non-empty news
        if news:
            NEWS_CACHE[company] = {"news": news, "timestamp": time.time()}
        else:
            # keep old cache if exists; if not exists, record it as fetched once with empty list
            if company not in NEWS_CACHE:
                NEWS_CACHE[company] = {"news": [], "timestamp": time.time()}
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
    # after each batch, rebuild sector cache for instant endpoints (keeps UI instant)
    try:
        rebuild_sector_cache()
    except Exception as e:
        logger.warning(f"rebuild_sector_cache failed after batch: {e}")

# -----------------------------
# Background loop
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
                logger.info(f"Updater: processing batch {i//BATCH_SIZE + 1} / {(total + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch)} companies)")
                await update_batch(batch)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            logger.info(f"Background update cycle finished. Cached companies: {len(NEWS_CACHE)}")
            # wait until next full cycle
            await asyncio.sleep(CACHE_DURATION)
        except Exception as e:
            logger.error(f"Background updater crashed: {e}")
            await asyncio.sleep(60)

# -----------------------------
# Builders for API sections
# -----------------------------
def _is_item_recent(item: Dict, days: int = RECENT_DAYS) -> bool:
    pub = item.get("pubDate","") or ""
    return is_recent(pub, days)

def build_all_section(limit=150):
    results = []
    added_companies = set()
    # 1) index news first (if present in cache)
    for company, cache in NEWS_CACHE.items():
        for item in cache.get("news", []):
            if not _is_item_recent(item): 
                continue
            txt = (item.get("title","") + " " + item.get("description","")).lower()
            if any(k in txt for k in INDEX_NEWS_KEYS):
                x = item.copy(); x["company"] = company
                results.append(x)
                added_companies.add(company)
                break
    # 2) top stocks - 1 item each, highest impact first
    for top in TOP_STOCKS:
        if top in NEWS_CACHE and top not in added_companies:
            candidates = [c for c in NEWS_CACHE[top].get("news", []) if _is_item_recent(c)]
            if not candidates:
                continue
            # sort by impact presence then by parsed date (newest first)
            candidates.sort(key=lambda it: (not is_high_impact(it.get("title","") + " " + it.get("description","")), -(parse_pubdate(it.get("pubDate","") or "") or datetime.min).timestamp()))
            chosen = candidates[0]
            x = chosen.copy(); x["company"] = top
            results.append(x)
            added_companies.add(top)
    # 3) fill remaining with other impactful companies (max 1 per company)
    for company, cache in NEWS_CACHE.items():
        if company in added_companies:
            continue
        for item in cache.get("news", []):
            if not _is_item_recent(item):
                continue
            if is_high_impact(item.get("title","") + " " + item.get("description","")):
                x = item.copy(); x["company"] = company
                results.append(x)
                added_companies.add(company)
                break
        if len(results) >= limit:
            break
    # 4) fill with diverse recent items if needed
    if len(results) < limit:
        for company, cache in NEWS_CACHE.items():
            if company in added_companies:
                continue
            for n in cache.get("news", []):
                if not _is_item_recent(n):
                    continue
                x = n.copy(); x["company"] = company
                results.append(x)
                added_companies.add(company)
                break
            if len(results) >= limit:
                break
    results = remove_duplicates(results)
    # try to sort by parsed pubDate desc (newest first)
    try:
        results.sort(key=lambda it: (parse_pubdate(it.get("pubDate","")) or datetime.min), reverse=True)
    except Exception:
        pass
    return results[:limit]

def build_results_section(limit=150):
    results = []
    added = set()
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            if not _is_item_recent(n):
                continue
            txt = (n.get("title","") + " " + n.get("description","")).lower()
            if is_results_news(txt) or any(k in txt for k in ["dividend", "buyback", "repurchase", "guidance", "forecast", "outlook"]):
                x = n.copy(); x["company"] = company
                if "facts" not in x:
                    x["facts"] = extract_financial_facts(txt)
                if "summary" not in x:
                    x["summary"] = generate_short_summary(x)
                results.append(x)
                added.add(company)
                break
        if len(results) >= limit:
            break
    try:
        results.sort(key=lambda it: (parse_pubdate(it.get("pubDate","")) or datetime.min), reverse=True)
    except Exception:
        pass
    return remove_duplicates(results)[:limit]

def build_sector_section(keywords: List[str], limit=150):
    items = []
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            if not _is_item_recent(n):
                continue
            txt = (n.get("title","") + " " + n.get("description","")).lower()
            if any(k.lower() in txt for k in keywords):
                x = n.copy(); x["company"] = company
                items.append(x)
    items = remove_duplicates(items)
    try:
        items.sort(key=lambda it: (parse_pubdate(it.get("pubDate","")) or datetime.min), reverse=True)
    except Exception:
        pass
    return items[:limit]

def build_penny_section(limit=150):
    items = []
    for p in PENNY_STOCKS:
        for n in NEWS_CACHE.get(p, {}).get("news", []):
            if not _is_item_recent(n):
                continue
            x = n.copy(); x["company"] = p
            items.append(x)
    items = remove_duplicates(items)
    try:
        items.sort(key=lambda it: (parse_pubdate(it.get("pubDate","")) or datetime.min), reverse=True)
    except Exception:
        pass
    return items[:limit]

def is_high_impact(text: str) -> bool:
    return any(k in text.lower() for k in IMPACT_KEYWORDS)

# -----------------------------
# Sector cache helpers
# -----------------------------
def rebuild_sector_cache():
    """Rebuild SECTOR_CACHE from NEWS_CACHE. Cheap and fast (in-memory)."""
    logger.info("Rebuilding sector cache...")
    # ALL
    SECTOR_CACHE["ALL"] = build_all_section(limit=500)
    # RESULTS
    SECTOR_CACHE["RESULTS"] = build_results_section(limit=500)
    # PENNY
    SECTOR_CACHE["PENNY"] = build_penny_section(limit=500)
    # LARGE CAP (top stocks)
    largecap_items = []
    for top in TOP_STOCKS:
        for n in NEWS_CACHE.get(top, {}).get("news", []):
            if not _is_item_recent(n):
                continue
            x = n.copy(); x["company"] = top
            largecap_items.append(x)
            break
    try:
        largecap_items.sort(key=lambda it: (parse_pubdate(it.get("pubDate","")) or datetime.min), reverse=True)
    except Exception:
        pass
    SECTOR_CACHE["LARGE CAP"] = remove_duplicates(largecap_items)[:500]
    # other sectors using keywords
    for sec, keys in SECTOR_KEYWORDS.items():
        SECTOR_CACHE[sec] = build_sector_section(keys, limit=500)
    # MIDCAP/SMALLCAP special placeholders (may use keywords)
    SECTOR_CACHE["MIDCAP"] = build_sector_section(["midcap"], limit=500)
    SECTOR_CACHE["SMALLCAP"] = build_sector_section(["smallcap"], limit=500)
    logger.info("Sector cache rebuilt.")

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
    # ensure sentiment and summary
    for n in news:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
        if "summary" not in n:
            n["summary"] = generate_short_summary(n)
        if "facts" not in n:
            n["facts"] = extract_financial_facts(n.get("title","") + " " + n.get("description",""))
    # Company-specific endpoint returns raw cached items (may include slightly older items)
    return {"company": company_name, "news": news}

@api_router.get("/news/all")
async def get_all_news():
    # return from SECTOR_CACHE for instant, fall back to building if not available
    items = SECTOR_CACHE.get("ALL")
    if items is None:
        items = build_all_section(limit=150)
        SECTOR_CACHE["ALL"] = items
    # ensure summary & sentiment exist
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
        if "summary" not in n:
            n["summary"] = generate_short_summary(n)
    return {"news": items[:150], "count": len(items)}

@api_router.get("/news/results")
async def get_results_news():
    items = SECTOR_CACHE.get("RESULTS")
    if items is None:
        items = build_results_section(limit=200)
        SECTOR_CACHE["RESULTS"] = items
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
        if "summary" not in n:
            n["summary"] = generate_short_summary(n)
    return {"news": items, "count": len(items)}

@api_router.get("/news/sector/{sector_name}")
async def get_sector_news(sector_name: str):
    s = sector_name.upper()
    # normalize a few names
    if s in ["PENNY", "PENNY STOCKS"]:
        key = "PENNY"
    elif s in ["LARGECAP", "LARGE CAP", "LARGE-CAP"]:
        key = "LARGE CAP"
    elif s in ["MIDCAP", "MID CAP"]:
        key = "MIDCAP"
    elif s in ["SMALLCAP", "SMALL CAP"]:
        key = "SMALLCAP"
    else:
        key = s
    items = SECTOR_CACHE.get(key)
    if items is None:
        # build on the fly (should be rare because sector cache is rebuilt frequently)
        if key == "PENNY":
            items = build_penny_section()
        elif key == "LARGE CAP":
            items = SECTOR_CACHE.get("LARGE CAP") or []
        elif key == "MIDCAP":
            items = build_sector_section(["midcap"])
        elif key == "SMALLCAP":
            items = build_sector_section(["smallcap"])
        elif key in SECTOR_KEYWORDS:
            items = build_sector_section(SECTOR_KEYWORDS[key])
        else:
            # fallback build by keyword substring
            items = build_sector_section([key.lower()])
        SECTOR_CACHE[key] = items
    # ensure enrichments
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
        if "summary" not in n:
            n["summary"] = generate_short_summary(n)
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

# -----------------------------
# App wiring
# -----------------------------
app.include_router(api_router)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# -----------------------------
# Startup & Shutdown
# -----------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("Server starting: loading companies and cache...")
    load_company_names()
    load_cache_from_file()
    # build sector cache from whatever is in cache already (instant)
    try:
        rebuild_sector_cache()
    except Exception as e:
        logger.warning(f"Initial sector cache build failed: {e}")
    # kick off background jobs
    asyncio.create_task(background_news_updater())
    asyncio.create_task(save_cache_periodically())
    logger.info("Startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Server shutting down: saving cache...")
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(NEWS_CACHE, f)
        logger.info("Shutdown saved cache")
    except Exception as e:
        logger.error(f"Error saving cache on shutdown: {e}")
    logger.info("Shutdown complete")
