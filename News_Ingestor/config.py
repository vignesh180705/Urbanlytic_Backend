# config.py
import os

# Google Cloud Project and Location
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'urbanlytic-466109')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'us-central1')

# Pub/Sub Topic Names
PROCESSED_EVENTS_TOPIC_NAME = os.environ.get('PROCESSED_EVENTS_TOPIC_NAME', 'processed-events')
ANALYTICS_SUGGESTIONS_TOPIC_NAME = os.environ.get('ANALYTICS_SUGGESTIONS_TOPIC_NAME', 'analytics-and-suggestions')

# NEW TOPIC FOR RAW NEWS DATA
RAW_NEWS_TOPIC_NAME = os.environ.get('RAW_NEWS_TOPIC_NAME', 'raw-news-posts')

# Gemini Model Configuration
GEMINI_MODEL_NAME = os.environ.get('GEMINI_MODEL_NAME', 'gemini-2.5-pro')
