# config.py
import os

# Google Cloud Project and Location
# Ensure these match your actual GCP project ID and the region where Gemini is available
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'oval-bot-472512-s4') 
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'us-central1') 

# Pub/Sub Topic Names
# These should match the topics you've configured in your GCP project
PROCESSED_EVENTS_TOPIC_NAME = os.environ.get('PROCESSED_EVENTS_TOPIC_NAME', 'processed-events') 
ANALYTICS_SUGGESTIONS_TOPIC_NAME = os.environ.get('ANALYTICS_SUGGESTIONS_TOPIC_NAME', 'analytics-and-suggestions')

# Gemini Model Configuration
# Ensure this matches the model name you want to use (e.g., 'gemini-2.5-pro' or 'gemini-pro')
GEMINI_MODEL_NAME = os.environ.get('GEMINI_MODEL_NAME', 'gemini-2.5-pro')