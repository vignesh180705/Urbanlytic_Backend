# news_ingestor/main.py (Cloud Run FastAPI)
import os
import json
import logging
import httpx # For making asynchronous HTTP requests
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration (from environment variables) ---
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'urbanlytic-466109')
# Use the new topic name from config.py
RAW_NEWS_TOPIC_NAME = os.environ.get('RAW_NEWS_TOPIC_NAME', 'raw-news-posts')

# NewsAPI.org credentials and parameters
# IMPORTANT: Replace with your actual API Key. Use Secret Manager in production.
NEWS_API_KEY = os.environ.get('NEWS_API_KEY', 'YOUR_NEWSAPI_KEY')
NEWS_API_BASE_URL = "https://newsapi.org/v2/everything"

# Search parameters for urban incidents in Chennai
# Keywords can be refined based on what you want to capture
DEFAULT_SEARCH_QUERY = os.environ.get('DEFAULT_SEARCH_QUERY', 'Chennai (traffic OR accident OR crime OR pothole OR flood OR pollution OR infrastructure)')
DEFAULT_LANGUAGE = os.environ.get('DEFAULT_LANGUAGE', 'en')
DEFAULT_COUNTRY = os.environ.get('DEFAULT_COUNTRY', 'in') # Filter by articles mentioning India, though 'everything' endpoint is global
DEFAULT_SORT_BY = os.environ.get('DEFAULT_SORT_BY', 'relevancy') # or 'publishedAt'
FETCH_INTERVAL_MINUTES = int(os.environ.get('FETCH_INTERVAL_MINUTES', '60')) # Fetch news from the last X minutes
MAX_ARTICLES_PER_RUN = int(os.environ.get('MAX_ARTICLES_PER_RUN', '50')) # Max articles to fetch per invocation

# Initialize FastAPI app
app = FastAPI()

# Initialize Pub/Sub Publisher Client globally
publisher = pubsub_v1.PublisherClient()

# Initialize HTTPX client globally for persistent connections
http_client = httpx.AsyncClient()

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

    try:
        # Calculate time from which to fetch news (e.g., last 60 minutes)
        from_time = datetime.now(timezone.utc) - timedelta(minutes=FETCH_INTERVAL_MINUTES)
        from_iso = from_time.isoformat(timespec='seconds').replace('+00:00', 'Z')

        params = {
            "q": DEFAULT_SEARCH_QUERY,
            "language": DEFAULT_LANGUAGE,
            "sortBy": DEFAULT_SORT_BY,
            "from": from_iso,
            "pageSize": MAX_ARTICLES_PER_RUN,
            "apiKey": NEWS_API_KEY
        }
        
        logger.info(f"Fetching news with query: '{DEFAULT_SEARCH_QUERY}' from {from_iso}")
        
        response = await http_client.get(NEWS_API_BASE_URL, params=params, timeout=30.0)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        
        news_data = response.json()
        articles = news_data.get('articles', [])

        if not articles:
            logger.info(f"No new articles found for query: '{DEFAULT_SEARCH_QUERY}' in the last {FETCH_INTERVAL_MINUTES} minutes.")
            return JSONResponse(content={"status": "No new articles"}, status_code=200)

        articles_published_count = 0
        topic_path = publisher.topic_path(GCP_PROJECT_ID, RAW_NEWS_TOPIC_NAME)

        for article in articles:
            # Basic validation and extraction
            if not article.get('title') or not article.get('url'):
                logger.warning(f"Skipping article due to missing title or URL: {article}")
                continue

            # Prepare article data for Pub/Sub
            article_data = {
                "title": article.get('title'),
                "description": article.get('description'),
                "url": article.get('url'),
                "publishedAt": article.get('publishedAt'),
                "source_name": article.get('source', {}).get('name'),
                "author": article.get('author'),
                "content": article.get('content'), # May be truncated
                "query_keywords": DEFAULT_SEARCH_QUERY,
                "ingestedAt": datetime.now(timezone.utc).isoformat() # Timestamp of ingestion
            }
            
            # Publish article data to Pub/Sub asynchronously
            message_data = json.dumps(article_data).encode('utf-8')
            future = publisher.publish(topic_path, message_data)
            await future # Await the future directly
            
            logger.info(f"Published article '{article.get('title')[:50]}...'")
            articles_published_count += 1

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
    load_dotenv() # Load .env for local testing
    
    port = int(os.environ.get('PORT', 8080))
    uvicorn.run(app, host='0.0.0.0', port=port)
