import json
import os
import time
import random
import asyncio
import logging
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

# ---------------------------------------------------------
# GLOBAL STATE
# ---------------------------------------------------------
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}
CACHE_FILE = ROOT_DIR / "news_cache.json"
CACHE_DURATION = 15 * 60  # 15 mins

# ---------------------------------------------------------
# SENTIMENT LOGIC
# ---------------------------------------------------------
def detect_sentiment(text: str):
    text = text.lower()

    good_words = [
        "profit", "growth", "surge", "wins", "acquisition", "expansion", "upgrade",
        "increase", "record high", "positive", "boost"
    ]
    bad_words = [
        "loss", "fraud", "scam", "crash", "down", "decline", "penalty",
        "investigation", "raid", "negative", "fall"
    ]

    if any(w in text for w in good_words):
        return "good"
    if any(w in text for w in bad_words):
        return "bad"
    return "neutral"

# ---------------------------------------------------------
# LOAD COMPANIES FROM PDF
# ---------------------------------------------------------
def load_company_names():
    global COMPANY_NAMES
    pdf_path = ROOT_DIR / "company_list.pdf"

    if not pdf_path.exists():
        logger.error(f"PDF not found: {pdf_path}")
        return

    try:
        with open(pdf_path, "rb") as f:
            pdf = PyPDF2.PdfReader(f)
            text = "".join(page.extract_text() for page in pdf.pages)

        companies = []
        for line in text.split("\n"):
            line = line.strip()
            if line and ("Limited" in line or "Ltd" in line or "ETF" in line):
                companies.append(line)

        seen = set()
        COMPANY_NAMES = [x for x in companies if not (x in seen or seen.add(x))]

        logger.info(f"Loaded {len(COMPANY_NAMES)} companies")
    except Exception as e:
        logger.error(f"Error loading company list: {e}")

# ---------------------------------------------------------
# PERSISTENT CACHE
# ---------------------------------------------------------
def load_cache_from_file():
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
                NEWS_CACHE.update(data)
            logger.info(f"Loaded {len(NEWS_CACHE)} cached companies from file.")
        except Exception as e:
            logger.error(f"Cache load error: {e}")


async def save_cache_periodically():
    while True:
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(NEWS_CACHE, f)
            logger.info(f"Saved cache: {len(NEWS_CACHE)} companies.")
        except Exception as e:
            logger.error(f"Cache save error: {e}")

        await asyncio.sleep(60)  # Save every 1 min

# ---------------------------------------------------------
# FETCH NEWS
# ---------------------------------------------------------
async def fetch_company_news(company_name: str) -> List[Dict]:
    try:
        url = f"https://news.google.com/rss/search?q={quote(company_name + ' stock')}"
        feed = await asyncio.to_thread(feedparser.parse, url)

        news = []
        for entry in feed.entries[:5]:
            news.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "pubDate": entry.get("published", ""),
                "description": entry.get("summary", ""),
            })
        return news
    except Exception as e:
        logger.error(f"Fetch error for {company_name}: {e}")
        return []

# ---------------------------------------------------------
# CACHE-FIRST LOGIC FOR USERS
# ---------------------------------------------------------
async def get_company_news_cached(company_name: str) -> List[Dict]:
    """User search MUST return cached data only."""
    if company_name in NEWS_CACHE:
        return NEWS_CACHE[company_name]["news"]
    return []

# ---------------------------------------------------------
# BACKGROUND UPDATER
# ---------------------------------------------------------
async def update_batch(companies: List[str]):
    semaphore = asyncio.Semaphore(10)

    async def fetch_one(company):
        async with semaphore:
            news = await fetch_company_news(company)

            # Add sentiment
            for n in news:
                text = (n.get("title", "") + " " + n.get("description", ""))
                n["sentiment"] = detect_sentiment(text)

            NEWS_CACHE[company] = {
                "news": news,
                "timestamp": time.time()
            }

    tasks = [fetch_one(name) for name in companies]
    await asyncio.gather(*tasks, return_exceptions=True)


async def background_news_updater():
    logger.info("Background updater started")

    while True:
        try:
            total = len(COMPANY_NAMES)
            batch_size = 100

            for i in range(0, total, batch_size):
                batch = COMPANY_NAMES[i:i + batch_size]
                await update_batch(batch)
                await asyncio.sleep(random.uniform(0.5, 1.5))

            logger.info(f"Cycle complete. Cached: {len(NEWS_CACHE)} companies.")
            await asyncio.sleep(CACHE_DURATION)

        except Exception as e:
            logger.error(f"Updater error: {e}")
            await asyncio.sleep(60)

# ---------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------
@api_router.get("/companies/search")
async def search_companies(q: str):
    q = q.lower()
    return [n for n in COMPANY_NAMES if n.lower().startswith(q)][:50]


@api_router.get("/news/company/{company_name}")
async def get_company_news(company_name: str):
    return {
        "company": company_name,
        "news": await get_company_news_cached(company_name)
    }


@api_router.get("/news/all")
async def all_news():
    all_items = []

    for company, cache in NEWS_CACHE.items():
        for n in cache["news"]:
            x = n.copy()
            x["company"] = company

            # Add sentiment if missing
            if "sentiment" not in x:
                x["sentiment"] = detect_sentiment(
                    x["title"] + " " + x.get("description", "")
                )

            all_items.append(x)

    return {"news": all_items[:150]}


@api_router.get("/news/sector/fmcg")
async def fmcg_news():
    items = []

    for company, cache in NEWS_CACHE.items():
        for n in cache["news"]:
            text = n["title"] + " " + n.get("description", "")
            if any(k.lower() in text.lower() for k in ["fmcg", "retail", "food"]):
                x = n.copy()
                x["company"] = company
                items.append(x)

    return {"news": items[:150]}


@api_router.get("/news/sector/health")
async def health_news():
    items = []

    for company, cache in NEWS_CACHE.items():
        for n in cache["news"]:
            text = n["title"] + " " + n.get("description", "")
            if any(k.lower() in text.lower() for k in ["pharma", "drug", "hospital"]):
                x = n.copy()
                x["company"] = company
                items.append(x)

    return {"news": items[:150]}


@api_router.get("/status")
async def get_status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE),
        "cache_duration_minutes": CACHE_DURATION / 60,
    }

# ---------------------------------------------------------
# STARTUP
# ---------------------------------------------------------
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    logger.info("Loading company names...")
    load_company_names()

    logger.info("Loading persistent cache...")
    load_cache_from_file()

    asyncio.create_task(background_news_updater())
    asyncio.create_task(save_cache_periodically())

    logger.info("Startup complete")


@app.on_event("shutdown")
async def shutdown():
    logger.info("Saving cache on shutdown...")
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(NEWS_CACHE, f)
    except:
        pass
    logger.info("Shutdown complete")
