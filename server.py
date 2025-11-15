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
from typing import Dict, List, Optional
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
NEWS_CACHE: Dict[str, Dict] = {}  # {company_name: {news: [...], timestamp: ..., }}
CACHE_DURATION = 15 * 60  # 15 minutes in seconds

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
    """Load company names from PDF"""
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
            
            # Extract lines and filter company names
            lines = text.split('\n')
            companies = []
            for line in lines:
                line = line.strip()
                # Skip empty lines, headers, and lines with "Limited" or "Ltd"
                if line and ('Limited' in line or 'Ltd' in line or 'ETF' in line):
                    companies.append(line)
            
            # Remove duplicates while preserving order
            seen = set()
            COMPANY_NAMES = [x for x in companies if not (x in seen or seen.add(x))]
            logger.info(f"Loaded {len(COMPANY_NAMES)} company names from PDF")
    except Exception as e:
        logger.error(f"Error loading company names: {e}")

# ============================================================================
# RSS FETCHING LOGIC
# ============================================================================
async def fetch_company_news(company_name: str) -> List[Dict]:
    """Fetch news for a specific company from Google News RSS"""
    try:
        query = f"{company_name} stock"
        url = f"https://news.google.com/rss/search?q={quote(query)}"
        
        # Fetch RSS feed
        feed = await asyncio.to_thread(feedparser.parse, url)
        
        news_items = []
        for entry in feed.entries[:5]:  # Take only top 5
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
    """Check if cache is still valid for a company"""
    if company_name not in NEWS_CACHE:
        return False
    
    cache_entry = NEWS_CACHE[company_name]
    cache_time = cache_entry.get('timestamp', 0)
    current_time = time.time()
    
    return (current_time - cache_time) < CACHE_DURATION

async def get_company_news_cached(company_name: str) -> List[Dict]:
    """Get company news with caching"""
    if is_cache_valid(company_name):
        return NEWS_CACHE[company_name]['news']
    
    # Fetch fresh news
    news = await fetch_company_news(company_name)
    
    # Update cache only if fetch succeeded
    if news:
        NEWS_CACHE[company_name] = {
            'news': news,
            'timestamp': time.time()
        }
    elif company_name in NEWS_CACHE:
        # Keep old cache if fetch failed
        return NEWS_CACHE[company_name]['news']
    
    return news

# ============================================================================
# BACKGROUND BATCH UPDATER
# ============================================================================
async def update_batch(companies: List[str]):
    """Update news for a batch of companies with concurrency control"""
    semaphore = asyncio.Semaphore(10)  # Limit concurrent requests
    
    async def fetch_with_limit(company):
        async with semaphore:
            return await get_company_news_cached(company)
    
    tasks = [fetch_with_limit(company) for company in companies]
    await asyncio.gather(*tasks, return_exceptions=True)

async def background_news_updater():
    """Background task to update news for all companies in batches"""
    logger.info("Starting background news updater...")
    
    while True:
        try:
            total_companies = len(COMPANY_NAMES)
            batch_size = 100
            
            logger.info(f"Starting update cycle for {total_companies} companies...")
            
            for i in range(0, total_companies, batch_size):
                batch = COMPANY_NAMES[i:i + batch_size]
                logger.info(f"Updating batch {i//batch_size + 1}: companies {i+1} to {i+len(batch)}")
                
                await update_batch(batch)
                
                # Random delay between batches (0.5s to 1.5s)
                delay = random.uniform(0.5, 1.5)
                await asyncio.sleep(delay)
            
            logger.info(f"Completed update cycle. Cached news for {len(NEWS_CACHE)} companies.")
            
            # Rest for 15 minutes before next cycle
            rest_duration = 15 * 60
            logger.info(f"Resting for {rest_duration/60} minutes...")
            await asyncio.sleep(rest_duration)
            
        except Exception as e:
            logger.error(f"Error in background updater: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying

# ============================================================================
# FILTERING LOGIC
# ============================================================================
def has_keywords(text: str, keywords: List[str]) -> bool:
    """Check if text contains any of the keywords (case-insensitive)"""
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)

def filter_impactful_news(all_news: List[Dict]) -> List[Dict]:
    """Filter news items for impactful content"""
    impactful = []
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=10)
    
    for item in all_news:
        # Check for impact keywords in title or description
        text = item['title'] + ' ' + item.get('description', '')
        if has_keywords(text, IMPACT_KEYWORDS):
            # Check date (last 7-10 days)
            try:
                # Parse various date formats
                pub_date_str = item.get('pubDate', '')
                if pub_date_str:
                    # This is a simple check, in production you'd parse the date properly
                    impactful.append(item)
            except:
                impactful.append(item)
    
    # Sort by newest (assuming pubDate is in descending order already)
    return impactful[:30]  # Return max 20-30 items

def filter_sector_news(all_news: List[Dict], keywords: List[str]) -> List[Dict]:
    """Filter news by sector keywords"""
    sector_news = []
    
    for item in all_news:
        text = item['title'] + ' ' + item.get('description', '')
        if has_keywords(text, keywords):
            sector_news.append(item)
    
    # Sort by newest, return up to 150 items
    return sector_news[:150]

# ============================================================================
# API ENDPOINTS
# ============================================================================
@api_router.get("/companies/search")
async def search_companies(q: str = Query("", description="Search query")):
    """Search companies by name prefix"""
    if not q:
        return []
    
    query_lower = q.lower()
    matches = [name for name in COMPANY_NAMES if name.lower().startswith(query_lower)]
    return matches[:50]  # Return top 50 matches

@api_router.get("/news/company/{company_name}")
async def get_news_by_company(company_name: str):
    """Get top 5 news for a specific company"""
    news = await get_company_news_cached(company_name)
    return {'company': company_name, 'news': news}

@api_router.get("/news/all")
async def get_all_impactful_news():
    """Get impactful news from all companies"""
    # Collect news from all cached companies
    all_news = []
    for company, cache_data in NEWS_CACHE.items():
        for news_item in cache_data.get('news', []):
            news_with_company = news_item.copy()
            news_with_company['company'] = company
            all_news.append(news_with_company)
    
    # Filter for impactful news
    impactful = filter_impactful_news(all_news)
    return {'news': impactful, 'count': len(impactful)}

@api_router.get("/news/sector/fmcg")
async def get_fmcg_news():
    """Get FMCG sector news"""
    # Collect all news
    all_news = []
    for company, cache_data in NEWS_CACHE.items():
        for news_item in cache_data.get('news', []):
            news_with_company = news_item.copy()
            news_with_company['company'] = company
            all_news.append(news_with_company)
    
    # Filter by FMCG keywords
    sector_news = filter_sector_news(all_news, FMCG_KEYWORDS)
    return {'news': sector_news, 'count': len(sector_news)}

@api_router.get("/news/sector/health")
async def get_health_news():
    """Get HEALTH sector news"""
    # Collect all news
    all_news = []
    for company, cache_data in NEWS_CACHE.items():
        for news_item in cache_data.get('news', []):
            news_with_company = news_item.copy()
            news_with_company['company'] = company
            all_news.append(news_with_company)
    
    # Filter by HEALTH keywords
    sector_news = filter_sector_news(all_news, HEALTH_KEYWORDS)
    return {'news': sector_news, 'count': len(sector_news)}

@api_router.get("/status")
async def get_status():
    """Get system status"""
    return {
        'companies_loaded': len(COMPANY_NAMES),
        'companies_cached': len(NEWS_CACHE),
        'cache_duration_minutes': CACHE_DURATION / 60
    }

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# STARTUP/SHUTDOWN EVENTS
# ============================================================================
@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    logger.info("Application starting up...")
    
    # Load company names from PDF
    load_company_names()
    
    # Start background updater
    asyncio.create_task(background_news_updater())
    
    logger.info("Application startup complete")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down...")
