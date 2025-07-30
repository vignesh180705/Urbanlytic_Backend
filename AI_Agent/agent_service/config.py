# config.py
import os

# Google Cloud Project and Location
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'urbanlytic-466109') # Replace with your actual project ID
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'us-central1') # Replace with your actual region

# Pub/Sub Topic Names
# Input topic for this AI Agent service
PROCESSED_EVENTS_TOPIC_NAME = os.environ.get('PROCESSED_EVENTS_TOPIC_NAME', 'processed-events')
# Output topic for this AI Agent service
ANALYTICS_SUGGESTIONS_TOPIC_NAME = os.environ.get('ANALYTICS_SUGGESTIONS_TOPIC_NAME', 'analytics-and-suggestions')

# Gemini Model Configuration
GEMINI_MODEL_NAME = os.environ.get('GEMINI_MODEL_NAME', 'gemini-pro')