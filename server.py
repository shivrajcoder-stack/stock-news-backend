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
CACHE_DURATION = 15 * 60  # 15 minutes rest between cycles
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

# locked sector order (as requested)
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
# Utilities & Rule-based extractors
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

# numeric helpers
_re_money = re.compile(r"(?:₹|Rs\.?|INR)\s?[\d,]+(?:\.\d+)?", flags=re.I)
_re_percent = re.compile(r"\d{1,3}\.\d+%|\d{1,3}%")
_re_number = re.compile(r"\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b")

# extract structured facts from text using rules
def extract_financial_facts(text: str) -> Dict:
    facts = {}
    if not text:
        return facts
    t = text

    # money values
    monies = _re_money.findall(t)
    if monies:
        facts["monies"] = monies

    # percents
    percents = _re_percent.findall(t)
    if percents:
        facts["percents"] = percents

    # EPS pattern
    m = re.search(r"\b(EPS|earnings per share)\b[: ]*\s*([₹Rs\.]*\s*\d+[\d,\.]*)", t, flags=re.I)
    if m:
        facts["eps"] = m.group(2).strip()

    # revenue / net profit matches
    rev = re.search(r"(?:revenue|sales)[^0-9\n]{0,20}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?)", t, flags=re.I)
    if rev:
        facts["revenue"] = rev.group(1).strip()

    profit = re.search(r"(?:net profit|profit after tax|PAT|net income)[^0-9\n]{0,20}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?)", t, flags=re.I)
    if profit:
        facts["net_profit"] = profit.group(1).strip()

    # dividend
    div = re.search(r"(?:dividend|payout)[^0-9\n]{0,20}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?(?:\s*/\s*share|\s*per\s*share)?)", t, flags=re.I)
    if div:
        facts["dividend"] = div.group(1).strip()

    # buyback
    buy = re.search(r"(?:buyback|repurchase)[^0-9\n]{0,40}([₹Rs\.]*\s*[\d,]+(?:\.\d+)?)", t, flags=re.I)
    if buy:
        facts["buyback_amount"] = buy.group(1).strip()

    # guidance phrase detect
    guidance_phrases = re.search(r"(?:guidance|expects|expects to|expects growth|outlook|forecast|target)\s*[:\-\sa-z0-9,()%]*", t, flags=re.I)
    if guidance_phrases:
        facts["guidance_snippet"] = guidance_phrases.group(0).strip()

    return facts

# detect if news is 'results' type
def is_results_news(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in ["q1", "q2", "q3", "q4", "quarter", "quarterly", "annual", "yearly", "results", "earnings", "net profit", "revenue", "pat", "eps"])

# find mentioned companies (best-effort): match company names
def find_companies_in_text(text: str, max_hits=12) -> List[str]:
    found = []
    tl = text.lower()
    # simple scan of company names (fast enough)
    for c in COMPANY_NAMES:
        if c.lower() in tl:
            found.append(c)
            if len(found) >= max_hits:
                break
    return found

def remove_html_and_trim(s: str) -> str:
    return clean_html(s)[:1000]  # limit length for summaries

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
            # Create concise summary: keep two lines max (title or short summary)
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
            n["summary"] = generate_short_summary(n)
            n["facts"] = extract_financial_facts(txt)
            # list of companies mentioned (helps list extraction)
            n["mentioned_companies"] = find_companies_in_text(txt)
        if news:
            NEWS_CACHE[company] = {"news": news, "timestamp": time.time()}
        elif company not in NEWS_CACHE:
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
                # small random sleep between batches to reduce burst
                await asyncio.sleep(random.uniform(0.5, 1.5))
            logger.info(f"Background update cycle finished. Cached companies: {len(NEWS_CACHE)}")
            # rest for configured duration before next full cycle
            await asyncio.sleep(CACHE_DURATION)
        except Exception as e:
            logger.error(f"Background updater crashed: {e}")
            await asyncio.sleep(60)

# -----------------------------
# Summary generator (rule-based)
# -----------------------------
def generate_short_summary(item: Dict) -> str:
    """
    Create a concise 1-2 line summary from title/description and extracted facts.
    """
    title = item.get("title", "") or ""
    desc = item.get("description", "") or ""
    txt = (title + " " + desc).strip()

    # If results-type, attempt to include revenue/profit/eps/dividend
    facts = extract_financial_facts(txt)
    parts = []

    # priority: facts (revenue/profit/dividend/eps/buyback)
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

    # if we have parts, join into short summary
    if parts:
        return " • ".join(parts)

    # if not, use the first concise sentence from description
    if desc:
        # pick first 1-2 sentences
        sentences = re.split(r'(?<=[\.\?\!])\s+', desc)
        if sentences and sentences[0].strip():
            s = sentences[0].strip()
            if len(s) > 180:
                s = s[:177].rsplit(' ',1)[0] + "..."
            return s
    # fallback to title
    if title:
        t = title
        if len(t) > 160:
            t = t[:157].rsplit(' ',1)[0] + "..."
        return t
    return ""

# -----------------------------
# Builders for API sections
# -----------------------------
def build_all_section(limit=150):
    """
    ALL section: index/news first, then TOP_STOCKS (1 item each), then other impactful companies.
    Ensures variety and recent-first ordering inside each group.
    """
    results = []
    added = set()

    # 1) index news first
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title","") + " " + n.get("description","")).lower()
            if any(k in txt for k in INDEX_NEWS_KEYS):
                item = n.copy(); item["company"] = company
                results.append(item)
                added.add(company)
                break

    # 2) top stocks
    for top in TOP_STOCKS:
        if top in NEWS_CACHE and top not in added:
            candidates = NEWS_CACHE[top].get("news", [])
            # pick the most 'impactful' item
            candidates = sorted(candidates, key=lambda it: (not is_high_impact(it.get("title","") + " " + it.get("description","")), it.get("pubDate","")), reverse=False)
            if candidates:
                chosen = candidates[0]
                x = chosen.copy(); x["company"] = top
                results.append(x)
                added.add(top)

    # 3) impactful others
    for company, cache in NEWS_CACHE.items():
        if company in added: continue
        for n in cache.get("news", []):
            if is_high_impact(n.get("title","") + " " + n.get("description","")):
                x = n.copy(); x["company"] = company
                results.append(x)
                added.add(company)
                break
        if len(results) >= limit:
            break

    # 4) fill with diverse recent items if needed
    if len(results) < limit:
        for company, cache in NEWS_CACHE.items():
            if company in added: continue
            for n in cache.get("news", []):
                x = n.copy(); x["company"] = company
                results.append(x)
                added.add(company)
                break
            if len(results) >= limit:
                break

    results = remove_duplicates(results)
    # try to sort by pubDate descending (best-effort)
    try:
        results.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    except:
        pass
    return results[:limit]

def build_results_section(limit=150):
    """
    Build RESULTS section: include quarterly/annual/dividend/buyback/guidance items.
    """
    results = []
    added = set()
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title","") + " " + n.get("description","")).lower()
            if is_results_news(txt) or any(k in txt for k in ["dividend", "buyback", "repurchase", "guidance", "forecast", "outlook"]):
                x = n.copy(); x["company"] = company
                # ensure structured facts are present
                if "facts" not in x:
                    x["facts"] = extract_financial_facts(txt)
                x["summary"] = generate_short_summary(x)
                results.append(x)
                added.add(company)
                break
        if len(results) >= limit:
            break

    # sort by recency
    try:
        results.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    except:
        pass
    return remove_duplicates(results)[:limit]

def build_sector_section(keywords: List[str], limit=150):
    items = []
    for company, cache in NEWS_CACHE.items():
        for n in cache.get("news", []):
            txt = (n.get("title","") + " " + n.get("description","")).lower()
            if any(k.lower() in txt for k in keywords):
                x = n.copy(); x["company"] = company
                items.append(x)
    items = remove_duplicates(items)
    try:
        items.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    except:
        pass
    return items[:limit]

def build_penny_section(limit=150):
    items = []
    for p in PENNY_STOCKS:
        for n in NEWS_CACHE.get(p, {}).get("news", []):
            x = n.copy(); x["company"] = p
            items.append(x)
    items = remove_duplicates(items)
    try:
        items.sort(key=lambda it: it.get("pubDate",""), reverse=True)
    except:
        pass
    return items[:limit]

def is_high_impact(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in IMPACT_KEYWORDS)

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
    return {"company": company_name, "news": news}

@api_router.get("/news/all")
async def get_all_news():
    items = build_all_section(limit=150)
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
        if "summary" not in n:
            n["summary"] = generate_short_summary(n)
    return {"news": items, "count": len(items)}

@api_router.get("/news/results")
async def get_results_news():
    items = build_results_section(limit=200)
    for n in items:
        if "sentiment" not in n:
            n["sentiment"] = detect_sentiment(n.get("title","") + " " + n.get("description",""))
        if "summary" not in n:
            n["summary"] = generate_short_summary(n)
    return {"news": items, "count": len(items)}

@api_router.get("/news/sector/{sector_name}")
async def get_sector_news(sector_name: str):
    s = sector_name.upper()
    if s == "PENNY":
        items = build_penny_section()
    elif s == "LARGECAP" or s == "LARGE CAP":
        # LARGE CAP -> top stocks
        items = []
        for top in TOP_STOCKS:
            for n in NEWS_CACHE.get(top, {}).get("news", []):
                x = n.copy(); x["company"] = top
                items.append(x)
    elif s == "MIDCAP":
        items = build_sector_section(["midcap"])
    elif s == "SMALLCAP":
        items = build_sector_section(["smallcap"])
    else:
        keywords = SECTOR_KEYWORDS.get(s, [sector_name])
        items = build_sector_section(keywords)

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
