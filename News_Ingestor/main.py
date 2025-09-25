import os
import json
import logging
import httpx
from datetime import datetime, timedelta, timezone 
import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from google.cloud import pubsub_v1
from google.cloud import firestore
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = 'oval-bot-472512-s4'
RAW_NEWS_TOPIC_NAME = 'raw-news-posts'

NEWS_API_KEY = 'fe151eb816a14267b4a6c5e2c30b50c3'
NEWS_API_BASE_URL = "https://newsapi.org/v2/everything"

DEFAULT_SEARCH_QUERY_ENV = os.environ.get('DEFAULT_SEARCH_QUERY', None)

QUERY_NAME_TO_USE = os.environ.get('QUERY_NAME_TO_USE', 'default_search_query')

db = firestore.Client()

from_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
HARDCODED_FROM_DATE = "2025-09-24T00:00:00Z"
HARDCODED_PAGE_SIZE = 1

DEFAULT_LANGUAGE = os.environ.get('DEFAULT_LANGUAGE', 'en')
DEFAULT_SORT_BY = os.environ.get('DEFAULT_SORT_BY', 'publishedAt') 

app = FastAPI()

publisher = pubsub_v1.PublisherClient()

http_client = httpx.AsyncClient()

_queries = {}
try:
    with open(os.path.join(os.path.dirname(__file__), 'queries.json'), 'r') as f:
        _queries = json.load(f)
    logger.info("Queries loaded from queries.json.")
except FileNotFoundError:
    logger.warning("queries.json not found. Falling back to hardcoded defaults for query names if no direct query is provided.")
    _queries = {
        "default_search_query": "Chennai (traffic OR accident OR crime OR pothole OR flood OR pollution OR infrastructure)",
        "test_search_query": "Chennai"
    }
except json.JSONDecodeError:
    logger.error("Error decoding queries.json. Check JSON syntax. Falling back to hardcoded defaults.")
    _queries = {
        "default_search_query": "Chennai (traffic OR accident OR crime OR pothole OR flood OR pollution OR infrastructure)",
        "test_search_query": "Chennai"
    }


@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

@app.post("/")
async def ingest_news(request: Request):
    """
    Cloud Run service endpoint to fetch news articles from NewsAPI.org
    and publish them to Pub/Sub.
    Triggered by HTTP (e.g., from Cloud Scheduler).
    """
    logger.info("News Ingestor Cloud Run service triggered.")

    if not NEWS_API_KEY or NEWS_API_KEY == 'YOUR_NEWSAPI_KEY':
        logger.error("NewsAPI.org API Key is not configured. Please set NEWS_API_KEY environment variable.")
        return JSONResponse(content={"error": "NewsAPI.org API Key not configured"}, status_code=500)

    if DEFAULT_SEARCH_QUERY_ENV:
        current_search_query = DEFAULT_SEARCH_QUERY_ENV
        logger.info(f"Using direct search query from environment variable: '{current_search_query}'")
    else:
        current_search_query = _queries.get(QUERY_NAME_TO_USE, _queries.get('default_search_query'))
        logger.info(f"Using search query from queries.json (via QUERY_NAME_TO_USE='{QUERY_NAME_TO_USE}'): '{current_search_query}'")

    if not current_search_query:
        logger.error("Search query could not be determined from environment or queries.json.")
        return JSONResponse(content={"error": "Search query not configured"}, status_code=500)


    try:
        params = {
            "q": current_search_query,
            "language": DEFAULT_LANGUAGE,
            "sortBy": DEFAULT_SORT_BY,
            "from": HARDCODED_FROM_DATE, 
            "pageSize": HARDCODED_PAGE_SIZE, 
            "page": 1,
            "apiKey": NEWS_API_KEY
        }
        
        logger.info(f"Fetching news with query: '{current_search_query}' from {HARDCODED_FROM_DATE} with pageSize={HARDCODED_PAGE_SIZE}")
        
        response = await http_client.get(NEWS_API_BASE_URL, params=params, timeout=30.0)
        response.raise_for_status() 
        
        news_data = response.json()
        articles = news_data.get('articles', [])

        if not articles:
            logger.info(f"No articles found for query: '{current_search_query}' from {HARDCODED_FROM_DATE} with pageSize={HARDCODED_PAGE_SIZE}.")
            return JSONResponse(content={"status": "No articles"}, status_code=200)

        articles_published_count = 0
        topic_path = publisher.topic_path(GCP_PROJECT_ID, RAW_NEWS_TOPIC_NAME)
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        published_today = (today_str)
        for article in articles:
            if not article.get('title') or not article.get('url'):
                logger.warning(f"Skipping article due to missing title or URL: {article}")
                continue

            article_data = {
                "title": article.get('title'),
                "description": article.get('description'),
                "url": article.get('url'),
                "publishedAt": article.get('publishedAt'),
                "source_name": article.get('source', {}).get('name'),
                "author": article.get('author'),
                "content": article.get('content'),
                "query_keywords": current_search_query,
                "ingestedAt": datetime.now(timezone.utc).isoformat()
            }
            
            message_data = json.dumps(article_data).encode('utf-8')
            #future = publisher.publish(topic_path, message_data)
            db.collection("news").add(article_data)
            logger.info(f"Published article '{article.get('title')[:50]}...' to Pub/Sub topic '{RAW_NEWS_TOPIC_NAME}'")
            articles_published_count += 1
            if articles_published_count >= HARDCODED_PAGE_SIZE:
                logger.info(f"Successfully published {articles_published_count} articles to Pub/Sub topic '{RAW_NEWS_TOPIC_NAME}'.")
                return JSONResponse(content={"status": f"Successfully published {articles_published_count} articles"}, status_code=200)
        logger.info(f"Successfully published {articles_published_count} articles to Pub/Sub topic '{RAW_NEWS_TOPIC_NAME}'.")
        return JSONResponse(content={"status": f"Successfully published {articles_published_count} articles"}, status_code=200)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching news: {e.response.status_code} - {e.response.text}", exc_info=True)
        return JSONResponse(content={"error": f"HTTP error fetching news: {e.response.status_code}"}, status_code=500)
    except httpx.RequestError as e:
        logger.error(f"Network error fetching news: {e}", exc_info=True)
        return JSONResponse(content={"error": f"Network error fetching news: {e}"}, status_code=500)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error from NewsAPI response: {e}", exc_info=True)
        return JSONResponse(content={"error": "Invalid JSON from NewsAPI"}, status_code=500)
    except Exception as e:
        logger.error(f"Unexpected error in news_ingestor: {e}", exc_info=True)
        return JSONResponse(content={"error": f"Internal Server Error: {e}"}, status_code=500)

# For local development
if __name__ == '__main__':
    import uvicorn
    from dotenv import load_dotenv
    load_dotenv()
    
    port = int(os.environ.get('PORT', 8080))
    uvicorn.run(app, host='0.0.0.0',port=port)

