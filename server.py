from fastapi import FastAPI, APIRouter, Query
from fastapi.responses import JSONResponse
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

# Create the main app without a prefix
app = FastAPI()

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================  
# GLOBAL DATA STRUCTURES  
# ============================================================================  
COMPANY_NAMES: List[str] = []
NEWS_CACHE: Dict[str, Dict] = {}  # {company_name: {news: [...], timestamp: ...}}
CACHE_DURATION = 15 * 60  # 15 minutes

# Impact keywords for ALL section
IMPACT_KEYWORDS = [
    "profit", "loss", "acquisition", "merger", "deal", "insider",
    "earnings", "quarterly results", "fraud", "FDI", "investment",
    "SEBI", "revenue", "scam"
]

# Sector keywords
FMCG_KEYWORDS = ["FMCG", "consumer goods", "food", "beverages", "retail", "packaged foods"]
HEALTH_KEYWORDS = ["pharma", "drug", "hospital", "healthcare", "biotech", "vaccine"]


# ============================================================================  
# PDF PARSING - Extract Company Names  
# ============================================================================  
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
            
            lines = text.split('\n')
            companies = []
            for line in lines:
                line = line.strip()
                if line and ('Limited' in line or 'Ltd' in line or 'ETF' in line):
                    companies.append(line)
            
            seen = set()
            COMPANY_NAMES = [x for x in companies if not (x in seen or seen.add(x))]
            logger.info(f"Loaded {len(COMPANY_NAMES)} company names from PDF")
    except Exception as e:
        logger.error(f"Error loading company names: {e}")


# ============================================================================  
# RSS FETCHING LOGIC  
# ============================================================================  
async def fetch_company_news(company_name: str) -> List[Dict]:
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        feed = await asyncio.to_thread(feedparser.parse, url)
        
        news_items = []
        for entry in feed.entries[:5]:
            news_items.append({
                'title': entry.get('title', ''),
                'link': entry.get('link', ''),
                'pubDate': entry.get('published', ''),
                'description': entry.get('summary', '')
            })
        
        return news_items
    except Exception as e:
        logger.error(f"Error fetching news for {company_name}: {e}")
        return []


def is_cache_valid(company_name: str) -> bool:
    if company_name not in NEWS_CACHE:
        return False
    
    cache_time = NEWS_CACHE[company_name].get('timestamp', 0)
    return (time.time() - cache_time) < CACHE_DURATION


# ============================================================================  
# FIXED FUNCTION — ALWAYS CACHES RESULTS  
# ============================================================================  
async def get_company_news_cached(company_name: str) -> List[Dict]:
    """Get company news with caching that ALWAYS stores results."""
    
    # Return cached data if valid
    if is_cache_valid(company_name):
        return NEWS_CACHE[company_name]['news']
    
    # Fetch fresh news
    news = await fetch_company_news(company_name)

    # ALWAYS cache (even empty lists)
    NEWS_CACHE[company_name] = {
        'news': news,
        'timestamp': time.time()
    }

    return news


# ============================================================================  
# BACKGROUND BATCH UPDATER  
# ============================================================================  
async def update_batch(companies: List[str]):
    semaphore = asyncio.Semaphore(10)
    
    async def fetch_with_limit(company):
        async with semaphore:
            return await get_company_news_cached(company)
    
    tasks = [fetch_with_limit(company) for company in companies]
    await asyncio.gather(*tasks, return_exceptions=True)


async def background_news_updater():
    logger.info("Starting background news updater...")
    
    while True:
        try:
            total = len(COMPANY_NAMES)
            batch_size = 100
            
            for i in range(0, total, batch_size):
                batch = COMPANY_NAMES[i:i + batch_size]
                await update_batch(batch)
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            logger.info(f"Cached news for {len(NEWS_CACHE)} companies.")
            await asyncio.sleep(15 * 60)
        
        except Exception as e:
            logger.error(f"Background updater error: {e}")
            await asyncio.sleep(60)


# ============================================================================  
# FILTERING  
# ============================================================================  
def has_keywords(text: str, keywords: List[str]) -> bool:
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)


def filter_impactful_news(all_news: List[Dict]) -> List[Dict]:
    impactful = []
    
    for item in all_news:
        text = item['title'] + ' ' + item.get('description', '')
        if has_keywords(text, IMPACT_KEYWORDS):
            impactful.append(item)
    
    return impactful[:30]


def filter_sector_news(all_news: List[Dict], keywords: List[str]) -> List[Dict]:
    sector_news = []
    
    for item in all_news:
        text = item['title'] + ' ' + item.get('description', '')
        if has_keywords(text, keywords):
            sector_news.append(item)
    
    return sector_news[:150]


# ============================================================================  
# API ENDPOINTS  
# ============================================================================  
@api_router.get("/companies/search")
async def search_companies(q: str = Query("", description="Search query")):
    if not q:
        return []
    
    q = q.lower()
    matches = [name for name in COMPANY_NAMES if name.lower().startswith(q)]
    return matches[:50]


@api_router.get("/news/company/{company_name}")
async def get_news_by_company(company_name: str):
    news = await get_company_news_cached(company_name)
    return {'company': company_name, 'news': news}


@api_router.get("/news/all")
async def get_all_impactful_news():
    all_news = []
    
    for company, cache_data in NEWS_CACHE.items():
        for item in cache_data.get('news', []):
            x = item.copy()
            x['company'] = company
            all_news.append(x)
    
    impactful = filter_impactful_news(all_news)
    return {'news': impactful, 'count': len(impactful)}


@api_router.get("/news/sector/fmcg")
async def get_fmcg_news():
    all_news = []
    for company, cache_data in NEWS_CACHE.items():
        for item in cache_data.get('news', []):
            x = item.copy()
            x['company'] = company
            all_news.append(x)
    
    sector = filter_sector_news(all_news, FMCG_KEYWORDS)
    return {'news': sector, 'count': len(sector)}


@api_router.get("/news/sector/health")
async def get_health_news():
    all_news = []
    for company, cache_data in NEWS_CACHE.items():
        for item in cache_data.get('news', []):
            x = item.copy()
            x['company'] = company
            all_news.append(x)
    
    sector = filter_sector_news(all_news, HEALTH_KEYWORDS)
    return {'news': sector, 'count': len(sector)}


@api_router.get("/status")
async def get_status():
    return {
        'companies_loaded': len(COMPANY_NAMES),
        'companies_cached': len(NEWS_CACHE),
        'cache_duration_minutes': CACHE_DURATION / 60
    }


# ============================================================================  
# APP SETUP  
# ============================================================================  
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================  
# STARTUP  
# ============================================================================  
@app.on_event("startup")
async def startup_event():
    logger.info("Starting app...")
    load_company_names()
    asyncio.create_task(background_news_updater())
    logger.info("Startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down...")
