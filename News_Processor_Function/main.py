# news_ingestor/main.py (Cloud Run FastAPI - with Timestamp Buffer)
import os
import json
import logging
import httpx
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration (from environment variables) ---
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'urbanlytic-466109')
RAW_NEWS_TOPIC_NAME = os.environ.get('RAW_NEWS_TOPIC_NAME', 'raw-news-posts')

# NewsAPI.org credentials and parameters
NEWS_API_KEY = os.environ.get('NEWS_API_KEY', 'YOUR_NEWSAPI_KEY')
NEWS_API_BASE_URL = "https://newsapi.org/v2/everything"

# Environment variable for direct search query (if provided via CLI)
DEFAULT_SEARCH_QUERY_ENV = os.environ.get('DEFAULT_SEARCH_QUERY', None)

# Environment variable to select which query to use from the JSON file (fallback)
QUERY_NAME_TO_USE = os.environ.get('QUERY_NAME_TO_USE', 'default_search_query')

FETCH_INTERVAL_MINUTES = int(os.environ.get('FETCH_INTERVAL_MINUTES', '60'))

# Explicitly define language and sort_by as environment variables
DEFAULT_LANGUAGE = os.environ.get('DEFAULT_LANGUAGE', 'en')
DEFAULT_SORT_BY = os.environ.get('DEFAULT_SORT_BY', 'relevancy')


# Initialize FastAPI app
app = FastAPI()

# Initialize Pub/Sub Publisher Client globally
publisher = pubsub_v1.PublisherClient()

# Initialize HTTPX client globally for persistent connections
http_client = httpx.AsyncClient()

# Load queries from JSON file at startup (will be used if DEFAULT_SEARCH_QUERY_ENV is not set)
_queries = {}
try:
    with open(os.path.join(os.path.dirname(__file__), 'queries.json'), 'r') as f:
        _queries = json.load(f)
    logger.info("Queries loaded from queries.json.")
except FileNotFoundError:
    logger.warning("queries.json not found. Falling back to hardcoded defaults for query names.")
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

    # Determine the search query: prioritize DEFAULT_SEARCH_QUERY_ENV, then QUERY_NAME_TO_USE, then default in JSON
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
        # Calculate time from which to fetch news (e.g., last 60 minutes + 5 minute buffer)
        # Adding a small buffer to ensure we don't miss articles due to indexing delays
        from_time = datetime.now(timezone.utc) - timedelta(minutes=FETCH_INTERVAL_MINUTES + 5) # <--- ADDED +5 MINUTE BUFFER
        from_iso = from_time.isoformat(timespec='seconds').replace('+00:00', 'Z')

        params = {
            "q": current_search_query,
            "language": DEFAULT_LANGUAGE,
            "sortBy": DEFAULT_SORT_BY,
            "from": from_iso,
            "pageSize": 1,
            "apiKey": NEWS_API_KEY
        }
        
        logger.info(f"Fetching news with query: '{current_search_query}' from {from_iso} with pageSize=1")
        
        response = await http_client.get(NEWS_API_BASE_URL, params=params, timeout=30.0)
        response.raise_for_status()
        
        news_data = response.json()
        articles = news_data.get('articles', [])

        if not articles:
            logger.info(f"No new articles found for query: '{current_search_query}' in the last {FETCH_INTERVAL_MINUTES} minutes.")
            return JSONResponse(content={"status": "No new articles"}, status_code=200)

        article = articles[0]
        
        if not article.get('title') or not article.get('url'):
            logger.warning(f"Skipping article due to missing title or URL: {article}")
            return JSONResponse(content={"status": "Skipped article due to missing data"}, status_code=200)

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
        topic_path = publisher.topic_path(GCP_PROJECT_ID, RAW_NEWS_TOPIC_NAME)
        future = publisher.publish(topic_path, message_data)
        await future
        
        logger.info(f"Successfully published ONE article '{article.get('title')[:50]}...' to Pub/Sub topic '{RAW_NEWS_TOPIC_NAME}'.")
        return JSONResponse(content={"status": f"Successfully published 1 article"}, status_code=200)

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
    uvicorn.run(app, host='0.0.0.0', port=port)
