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
# Top / Nifty lists
# -----------------------------
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

PENNY_STOCKS = [
    "Tilaknagar Industries Limited",
    "3i Infotech Limited",
    "XYZ Penny Ltd"
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
    "profit", "record", "growth", "surge", "beats", "upgrade",
    "wins", "strong", "rise", "positive", "acquisition", "expansion"
]

BAD_KEYWORDS = [
    "loss", "fraud", "scam", "crash", "decline", "penalty",
    "investigation", "downgrade", "fall", "weak", "slump", "lawsuit"
]

IMPACT_KEYWORDS = GOOD_KEYWORDS + BAD_KEYWORDS + [
    "earnings", "results", "investment", "SEBI", "revenue"
]

def clean_html(text: str) -> str:
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
        key = (n.get("title", "").lower(), n.get("link", "").lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(n)

    return out


def load_company_names():
    global COMPANY_NAMES
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
        logger.info(f"Loaded {len(COMPANY_NAMES)} companies")

    except:
        logger.error("PDF load failed")


def load_cache_from_file():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            NEWS_CACHE.update(data)
            logger.info(f"Loaded cache: {len(NEWS_CACHE)} companies")
        except:
            logger.error("Cache load failed")


async def save_cache_periodically():
    while True:
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(NEWS_CACHE, f)
            logger.info(f"Saved cache ({len(NEWS_CACHE)})")
        except:
            logger.error("Cache save failed")

        await asyncio.sleep(SAVE_INTERVAL_SECONDS)


async def fetch_company_news(company_name: str):
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"

        feed = await asyncio.to_thread(feedparser.parse, url)

        items = []
        for e in feed.entries[:8]:
            title = clean_html(e.get("title", ""))
            summary = clean_html(e.get("summary", ""))
            link = e.get("link", "")
            pub = e.get("published", "")

            items.append({
                "title": title,
                "description": summary,
                "link": link,
                "pubDate": pub
            })

        items = remove_duplicates(items)
        return items[:5]

    except:
        return []


async def update_one_company(company):
    news = await fetch_company_news(company)

    for n in news:
        txt = n["title"] + " " + n["description"]
        n["sentiment"] = detect_sentiment(txt)

    if news:
        NEWS_CACHE[company] = {
            "news": news,
            "timestamp": time.time()
        }
    elif company not in NEWS_CACHE:
        NEWS_CACHE[company] = {
            "news": [],
            "timestamp": time.time()
        }


async def update_batch(companies):
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async def worker(c):
        async with sem:
            await update_one_company(c)

    await asyncio.gather(*[worker(c) for c in companies])


async def background_news_updater():
    logger.info("Background updater started")

    while True:
        total = len(COMPANY_NAMES)

        for i in range(0, total, BATCH_SIZE):
            batch = COMPANY_NAMES[i:i + BATCH_SIZE]
            logger.info(
                f"Updater batch {i//BATCH_SIZE+1}/{(total+BATCH_SIZE-1)//BATCH_SIZE}"
            )
            await update_batch(batch)
            await asyncio.sleep(random.uniform(0.5, 1.5))

        logger.info(f"Cycle complete, cached: {len(NEWS_CACHE)}")
        await asyncio.sleep(CACHE_DURATION)
