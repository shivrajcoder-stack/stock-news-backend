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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("server")

# -----------------------------
# CONFIG
# -----------------------------
CACHE_FILE = ROOT_DIR / "news_cache.json"
COMPANY_PDF = ROOT_DIR / "company_list.pdf"
CACHE_DURATION = 15 * 60
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60

# -----------------------------
# GLOBAL STATE
# -----------------------------
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}
INDEX_NEWS_KEYS = ["nifty", "sensex", "banknifty", "nifty bank", "index"]

TOP_STOCKS = [
    "Reliance Industries Limited", "Tata Consultancy Services Limited",
    "HDFC Bank Limited", "ICICI Bank Limited", "Infosys Limited",
    "Hindustan Unilever Limited", "State Bank of India",
    "Larsen & Toubro Limited", "Bharti Airtel Limited", "ITC Limited",
    "Tata Motors Limited", "Kotak Mahindra Bank Limited",
    "Axis Bank Limited", "Maruti Suzuki India Limited",
    "Bajaj Finance Limited", "Mahindra & Mahindra Limited",
    "Wipro Limited", "Power Grid Corporation of India Limited",
    "Asian Paints Limited", "HCL Technologies Limited"
]

PENNY_STOCKS = [
    "Tilaknagar Industries Limited", "3i Infotech Limited", "XYZ Penny Ltd"
]

SECTOR_KEYWORDS = {
    "FMCG": ["fmcg", "consumer goods", "food", "beverage"],
    "IT": ["it", "software", "technology", "digital"],
    "BANKING": ["bank", "banking", "sbi", "hdfc"],
    "AUTO": ["auto", "vehicle", "motors"],
    "ENERGY": ["energy", "oil", "petro", "gas"],
    "PSU": ["psu", "public sector"],
    "TELECOM": ["telecom", "airtel", "vodafone"],
    "MIDCAP": ["midcap"],
    "SMALLCAP": ["smallcap"],
}

GOOD = ["profit", "growth", "surge", "upgrade", "strong"]
BAD = ["loss", "fraud", "fall", "decline", "scam"]
IMPACT = GOOD + BAD + ["results", "earnings", "revenue"]

# -----------------------------
# HELPERS
# -----------------------------
def clean_html(t):
    if not t: return ""
    t = re.sub(r"<[^>]+>", "", t)
    return re.sub(r"\s+", " ", t).strip()

def detect_sentiment(t):
    t = t.lower()
    if any(w in t for w in GOOD): return "good"
    if any(w in t for w in BAD): return "bad"
    return "neutral"

def remove_duplicates(items):
    seen = set()
    out = []
    for it in items:
        key = (it.get("title",""), it.get("link",""))
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out

# -----------------------
# DATETIME SANITIZER 🔥🔥
# -----------------------
def normalize_news_cache():
    for company, data in NEWS_CACHE.items():
        for item in data.get("news", []):
            d = item.get("pubDate")
            if not isinstance(d, str):
                item["pubDate"] = str(d)

# -----------------------------
# LOAD COMPANIES FROM PDF
# -----------------------------
def load_company_names():
    global COMPANY_NAMES
    if not COMPANY_PDF.exists():
        logger.error("company_list.pdf missing!")
        return
    try:
        with open(COMPANY_PDF, "rb") as f:
            pdf = PyPDF2.PdfReader(f)
            text = "".join([p.extract_text() or "" for p in pdf.pages])
        names = []
        for line in text.split("\n"):
            line = line.strip()
            if "Limited" in line or "Ltd" in line:
                names.append(line)
        COMPANY_NAMES = list(dict.fromkeys(names))
        logger.info(f"Loaded {len(COMPANY_NAMES)} companies")
    except Exception as e:
        logger.error(f"PDF load error: {e}")

# -----------------------------
# SAVE / LOAD CACHE
# -----------------------------
def load_cache_from_file():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as f:
                data = json.load(f)
                if isinstance(data, dict):
                    NEWS_CACHE.update(data)
            logger.info(f"Cache loaded: {len(NEWS_CACHE)}")
        except Exception as e:
            logger.error(f"Cache load error: {e}")

async def save_cache_periodically():
    while True:
        try:
            normalize_news_cache()   # FIX 🔥
            with open(CACHE_FILE, "w") as f:
                json.dump(NEWS_CACHE, f)
            logger.info("Cache saved")
        except Exception as e:
            logger.error(f"Save cache error: {e}")
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)

# -----------------------------
# FETCH NEWS
# -----------------------------
async def fetch_news(company):
    try:
        url = f"https://news.google.com/rss/search?q={quote(company + ' stock')}"
        feed = await asyncio.to_thread(feedparser.parse, url)
        out = []
        for e in feed.entries[:8]:
            title = clean_html(e.get("title"))
            desc = clean_html(e.get("summary") or e.get("description"))
            out.append({
                "title": title,
                "description": desc,
                "link": e.get("link"),
                "pubDate": e.get("published") or e.get("updated") or "",
                "sentiment": detect_sentiment(title + " " + desc)
            })
        return remove_duplicates(out)[:5]
    except Exception as e:
        logger.error(f"Fetch error {company}: {e}")
        return []

# -----------------------------
# UPDATE ONE
# -----------------------------
async def update_one(company):
    news = await fetch_news(company)
    if news:
        NEWS_CACHE[company] = {"news": news, "timestamp": time.time()}
    elif company not in NEWS_CACHE:
        NEWS_CACHE[company] = {"news": [], "timestamp": time.time()}

# -----------------------------
# BACKGROUND LOOP
# -----------------------------
async def background_news_updater():
    logger.info("Updater started")
    while True:
        total = len(COMPANY_NAMES)
        for i in range(0, total, BATCH_SIZE):
            batch = COMPANY_NAMES[i:i+BATCH_SIZE]
            sem = asyncio.Semaphore(SEMAPHORE_LIMIT)

            async def worker(c):
                async with sem:
                    await update_one(c)

            await asyncio.gather(*[worker(c) for c in batch])
            await asyncio.sleep(random.uniform(0.3,1))

        logger.info("Cycle complete")
        await asyncio.sleep(CACHE_DURATION)

# -----------------------------
# BUILDERS (NO LAG)
# -----------------------------
def last_7_days(item):
    try:
        return "202" in item.get("pubDate","")
    except:
        return True

def build_all():
    items = []
    for c, data in NEWS_CACHE.items():
        for n in data.get("news", []):
            if last_7_days(n):
                x = n.copy(); x["company"] = c
                items.append(x)
    try:
        items.sort(key=lambda x: x.get("pubDate",""), reverse=True)
    except:
        pass
    return remove_duplicates(items)[:150]

def build_sector(keys):
    out = []
    keys = [k.lower() for k in keys]
    for c, data in NEWS_CACHE.items():
        for n in data.get("news", []):
            t = (n["title"] + " " + n["description"]).lower()
            if any(k in t for k in keys):
                x=n.copy();x["company"]=c
                out.append(x)
    try:
        out.sort(key=lambda x: x.get("pubDate",""), reverse=True)
    except:
        pass
    return remove_duplicates(out)[:150]

def build_penny():
    out=[]
    for p in PENNY_STOCKS:
        for n in NEWS_CACHE.get(p,{}).get("news",[]):
            x=n.copy();x["company"]=p
            out.append(x)
    return remove_duplicates(out)[:150]

# -----------------------------
# API ENDPOINTS
# -----------------------------
@api_router.get("/news")
async def api_all():
    normalize_news_cache()
    return {"news": build_all()}

@api_router.get("/news/results")
async def api_results():
    out=[]
    for c,data in NEWS_CACHE.items():
        for n in data.get("news",[]):
            t=(n["title"]+" "+n["description"]).lower()
            if any(k in t for k in ["q1","q2","q3","q4","quarter","annual","results"]):
                x=n.copy();x["company"]=c
                out.append(x)
    try:
        out.sort(key=lambda x: x.get("pubDate",""), reverse=True)
    except: pass
    return {"news": out[:200]}

@api_router.get("/news/sector/{name}")
async def api_sector(name: str):
    s=name.upper()
    if s=="PENNY": return {"news": build_penny()}
    if s=="LARGE CAP": return {"news": build_sector(["reliance","hdfc","tcs","axis"])}
    if s in SECTOR_KEYWORDS:
        return {"news": build_sector(SECTOR_KEYWORDS[s])}
    return {"news":[]}

@api_router.get("/news/company/{company}")
async def api_company(company:str):
    news = NEWS_CACHE.get(company,{}).get("news",[])
    normalize_news_cache()
    return {"company": company, "news": news}

@api_router.get("/companies/search")
async def api_search(q: str = ""):
    ql = q.lower()
    res=[n for n in COMPANY_NAMES if ql in n.lower()]
    return res[:50]

@api_router.get("/status")
async def api_status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE)
    }

# -----------------------------
# APP WIRING
# -----------------------------
app.include_router(api_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# -----------------------------
# STARTUP
# -----------------------------
@app.on_event("startup")
async def startup():
    load_company_names()
    load_cache_from_file()
    asyncio.create_task(background_news_updater())
    asyncio.create_task(save_cache_periodically())
    logger.info("Startup complete")
