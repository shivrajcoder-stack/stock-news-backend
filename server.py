from fastapi import FastAPI, APIRouter, Query
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
import feedparser
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import time
import random
from urllib.parse import quote
import PyPDF2

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==============================
# GLOBAL DATA
# ==============================
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}  # {company_name: {news: [...], timestamp: ...}}
CACHE_DURATION = 15 * 60  # 15 minutes

IMPACT_KEYWORDS = [
    "profit", "loss", "acquisition", "merger", "deal", "insider",
    "earnings", "quarterly results", "fraud", "FDI", "investment",
    "SEBI", "revenue", "scam"
]

FMCG_KEYWORDS = ["FMCG", "consumer goods", "food", "beverages", "retail", "packaged foods"]
HEALTH_KEYWORDS = ["pharma", "drug", "hospital", "healthcare", "biotech", "vaccine"]


# ==============================
# LOAD COMPANY LIST FROM PDF
# ==============================
def load_company_names():
    global COMPANY_NAMES
    pdf_path = ROOT_DIR / 'company_list.pdf'
    
    if not pdf_path.exists():
        logger.error(f"PDF file not found at {pdf_path}")
        return
    
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text()
            
            companies = []
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if line and ('Limited' in line or 'Ltd' in line or 'ETF' in line):
                    companies.append(line)
            
            seen = set()
            COMPANY_NAMES = [x for x in companies if not (x in seen or seen.add(x))]
            logger.info(f"Loaded {len(COMPANY_NAMES)} companies")
    except Exception as e:
        logger.error(f"Error loading company names: {e}")


# ==============================
# GOOGLE NEWS FETCH
# ==============================
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
                "description": entry.get("summary", "")
            })
        return news
    except Exception as e:
        logger.error(f"Error fetching news for {company_name}: {e}")
        return []


# ==============================
# NEW LOGIC — CACHE ALWAYS, NEVER FETCH ON USER REQUEST
# ==============================
async def get_company_news_cached(company_name: str) -> List[Dict]:
    """
    NEW LOGIC:
    ✔ NEVER fetch when user searches
    ✔ ALWAYS return cached result instantly
    ✔ If not cached yet → return empty list
    """
    if company_name in NEWS_CACHE:
        return NEWS_CACHE[company_name]["news"]

    return []  # no fetching on user request


# ==============================
# BACKGROUND UPDATER (FETCHES EVERYTHING)
# ==============================
async def update_batch(companies: List[str]):
    semaphore = asyncio.Semaphore(10)

    async def fetch_with_limit(name):
        async with semaphore:
            news = await fetch_company_news(name)
            NEWS_CACHE[name] = {
                "news": news,
                "timestamp": time.time()
            }

    tasks = [fetch_with_limit(c) for c in companies]
    await asyncio.gather(*tasks, return_exceptions=True)


async def background_news_updater():
    logger.info("Background updater started")

    while True:
        try:
            total = len(COMPANY_NAMES)
            batch_size = 100

            for i in range(0, total, batch_size):
                batch = COMPANY_NAMES[i:i+batch_size]
                await update_batch(batch)
                await asyncio.sleep(random.uniform(0.5, 1.5))

            logger.info(f"Updated cache for {len(NEWS_CACHE)} companies")
            await asyncio.sleep(15 * 60)

        except Exception as e:
            logger.error(f"Updater error: {e}")
            await asyncio.sleep(60)


# ==============================
# FILTERING FUNCTIONS
# ==============================
def has_keywords(text: str, keywords: List[str]) -> bool:
    text = text.lower()
    return any(k.lower() in text for k in keywords)


def filter_impactful_news(all_news: List[Dict]):
    impactful = []
    for item in all_news:
        text = item["title"] + " " + item.get("description", "")
        if has_keywords(text, IMPACT_KEYWORDS):
            impactful.append(item)
    return impactful[:30]


def filter_sector_news(all_news: List[Dict], keywords: List[str]):
    result = []
    for item in all_news:
        if has_keywords(item["title"] + " " + item.get("description", ""), keywords):
            result.append(item)
    return result[:150]


# ==============================
# API ENDPOINTS
# ==============================
@api_router.get("/companies/search")
async def search_companies(q: str = Query("", description="Search query")):
    if not q:
        return []
    q = q.lower()
    return [n for n in COMPANY_NAMES if n.lower().startswith(q)][:50]


@api_router.get("/news/company/{company_name}")
async def get_company_news(company_name: str):
    news = await get_company_news_cached(company_name)
    return {"company": company_name, "news": news}


@api_router.get("/news/all")
async def get_all_news():
    all_news = []
    for company, cache in NEWS_CACHE.items():
        for item in cache["news"]:
            x = item.copy()
            x["company"] = company
            all_news.append(x)
    return {"news": filter_impactful_news(all_news)}


@api_router.get("/news/sector/fmcg")
async def get_fmcg_news():
    all_news = []
    for company, cache in NEWS_CACHE.items():
        for item in cache["news"]:
            x = item.copy()
            x["company"] = company
            all_news.append(x)
    return {"news": filter_sector_news(all_news, FMCG_KEYWORDS)}


@api_router.get("/news/sector/health")
async def get_health_news():
    all_news = []
    for company, cache in NEWS_CACHE.items():
        for item in cache["news"]:
            x = item.copy()
            x["company"] = company
            all_news.append(x)
    return {"news": filter_sector_news(all_news, HEALTH_KEYWORDS)}


@api_router.get("/status")
async def get_status():
    return {
        "companies_loaded": len(COMPANY_NAMES),
        "companies_cached": len(NEWS_CACHE),
        "cache_duration_minutes": CACHE_DURATION / 60,
    }


# ==============================
# STARTUP
# ==============================
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    logger.info("Loading company names...")
    load_company_names()
    asyncio.create_task(background_news_updater())
    logger.info("Startup complete")


@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down...")
