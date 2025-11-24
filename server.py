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

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------------
# CONFIG
# -----------------------------
CACHE_FILE = ROOT_DIR / "news_cache.json"
COMPANY_PDF = Path("/mnt/data/combined_companies.pdf")   # patched
CACHE_DURATION = 15 * 60
BATCH_SIZE = 100
SEMAPHORE_LIMIT = 10
SAVE_INTERVAL_SECONDS = 60


# -----------------------------
# GLOBALS
# -----------------------------
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}

INDEX_NEWS_KEYS = [
    "nifty", "sensex", "banknifty", "nifty bank",
    "nifty50", "nifty 50", "nifty it", "nifty auto",
    "nifty pharma", "index"
]

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

# Pseudo index companies
INDEX_COMPANIES = [
    "INDEX_NIFTY",
    "INDEX_SENSEX",
    "INDEX_BANKNIFTY"
]

GOOD_KEYWORDS = [
    "profit", "record", "growth", "surge", "beats", "upgrade",
    "wins", "strong", "rise", "positive", "acquisition", "expansion"
]

BAD_KEYWORDS = [
    "loss", "fraud", "scam", "crash", "decline", "penalty",
    "investigation", "downgrade", "fall", "weak", "slump", "lawsuit"
]

IMPACT_KEYWORDS = GOOD_KEYWORDS + BAD_KEYWORDS + [
    "earnings", "results", "investment", "sebi", "revenue"
]


# -----------------------------
# UTILITIES
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


def remove_duplicates(items):
    seen = set()
    out = []
    for x in items:
        key = (x.get("title", "").strip().lower(), x.get("link", "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
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


def within_days(item, days):
    if not days:
        return True
    pub = item.get("pubDate", "")
    dt = parse_date(pub)
    if not dt:
        return True
    delta = datetime.now(timezone.utc) - dt
    return delta.days <= days


# -----------------------------
# LOAD COMPANIES (PDF)
# -----------------------------
def load_company_names():
    global COMPANY_NAMES

    COMPANY_NAMES = TOP_STOCKS + INDEX_COMPANIES
    return  # in Super Clean mode, skip PDF parsing


# -----------------------------
# FETCH INDEX NEWS
# -----------------------------
async def fetch_index_news(index_type: str) -> List[Dict]:
    query_map = {
        "INDEX_NIFTY": "Nifty 50 index",
        "INDEX_SENSEX": "Sensex index",
        "INDEX_BANKNIFTY": "Bank Nifty index"
    }

    query = quote(query_map.get(index_type, "Nifty 50 index"))
    url = f"https://news.google.com/rss/search?q={query}"

    try:
        feed = await asyncio.to_thread(feedparser.parse, url)
        out = []

        for entry in feed.entries[:8]:
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", ""))
            link = entry.get("link", "")
            pub = entry.get("published", "")

            out.append({
                "title": title,
                "description": summary,
                "link": link,
                "pubDate": parse_date(pub).isoformat() if pub else "",
                "company": index_type,
                "sentiment": detect_sentiment(title + " " + summary)
            })

        return remove_duplicates(out)

    except Exception as e:
        logger.error(f"Index fetch failed for {index_type}: {e}")
        return []


# -----------------------------
# FETCH COMPANY NEWS
# -----------------------------
async def fetch_company_news(company: str):
    if company in INDEX_COMPANIES:
        return await fetch_index_news(company)

    try:
        query = quote(f"{company} stock")
        url = f"https://news.google.com/rss/search?q={query}"
        feed = await asyncio.to_thread(feedparser.parse, url)

        out = []
        for entry in feed.entries[:8]:
            title = clean_html(entry.get("title", ""))
            summary = clean_html(entry.get("summary", ""))
            link = entry.get("link", "")
            pub = entry.get("published", "")

            out.append({
                "title": title,
                "description": summary,
                "link": link,
                "pubDate": parse_date(pub).isoformat() if pub else "",
                "company": company,
                "sentiment": detect_sentiment(title + " " + summary)
            })

        return remove_duplicates(out[:5])

    except Exception as e:
        logger.error(f"Fetch company error: {e}")
        return []


# -----------------------------
# UPDATE every company
# -----------------------------
async def update_one_company(company):
    try:
        news = await fetch_company_news(company)
        NEWS_CACHE[company] = {
            "news": news,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"update_one_company failed: {e}")


async def background_news_updater():
    logger.info("Background updater started")
    while True:
        try:
            for c in COMPANY_NAMES:
                await update_one_company(c)
                await asyncio.sleep(0.5)

            logger.info("Cycle completed.")
            await asyncio.sleep(CACHE_DURATION)

        except Exception as e:
            logger.error(f"background updater crash: {e}")
            await asyncio.sleep(10)


# -----------------------------
# SECTION BUILDERS
# -----------------------------
def build_index_section():
    out = []
    for idx in INDEX_COMPANIES:
        out.extend(NEWS_CACHE.get(idx, {}).get("news", []))
    return remove_duplicates(out)


def build_largecap_section():
    out = []
    for c in TOP_STOCKS:
        out.extend(NEWS_CACHE.get(c, {}).get("news", []))
    return remove_duplicates(out)


def build_general_section():
    out = []
    for comp, data in NEWS_CACHE.items():
        if comp in TOP_STOCKS or comp in INDEX_COMPANIES:
            continue
        out.extend(data.get("news", []))
    return remove_duplicates(out)



# -----------------------------
# API ENDPOINTS
# -----------------------------
@api_router.get("/news/all")
async def news_all(include_indexes: bool = False):
    if include_indexes:
        return {
            "sections": {
                "indexes": build_index_section(),
                "largecap": build_largecap_section(),
                "general": build_general_section()
            }
        }

    flat = (
        build_index_section()
        + build_largecap_section()
        + build_general_section()
    )
    return {"news": remove_duplicates(flat)}


@api_router.get("/news/company/{company}")
async def news_company(company: str):
    return {"company": company, "news": NEWS_CACHE.get(company, {}).get("news", [])}


@api_router.get("/companies/search")
async def search_company(q: str):
    ql = q.lower()
    matches = [c for c in COMPANY_NAMES if ql in c.lower()]
    return matches[:50]


# Status
@api_router.get("/status")
async def status():
    return {
        "companies": COMPANY_NAMES,
        "cache_size": len(NEWS_CACHE)
    }


# -----------------------------
# APP INIT
# -----------------------------
app.include_router(api_router)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def start():
    load_company_names()
    asyncio.create_task(background_news_updater())
