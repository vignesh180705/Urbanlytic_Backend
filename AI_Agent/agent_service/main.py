# main.py
import asyncio
import os
import json
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.cloud import firestore
from agents.incident_analyzer_agent import IncidentAnalyzerAgent
from agents.mood_analyzer_agent import MoodAnalyzerAgent
from agents.crime_analyzer_agent import CrimeAnalyzerAgent
from config import GCP_PROJECT_ID, PROCESSED_EVENTS_TOPIC_NAME, ANALYTICS_SUGGESTIONS_TOPIC_NAME, GCP_LOCATION, GEMINI_MODEL_NAME
import vertexai
from wsgi2asgi import WSGI2ASGI # <--- ADD THIS IMPORT

# Load environment variables from .env file
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Initialize Google Cloud clients
publisher = pubsub_v1.PublisherClient()
db = firestore.Client(project=GCP_PROJECT_ID)

# Initialize Vertex AI
vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)

# Initialize AI Agents
incident_analyzer = IncidentAnalyzerAgent(project_id=GCP_PROJECT_ID, location=GCP_LOCATION, model_name=GEMINI_MODEL_NAME)
mood_analyzer = MoodAnalyzerAgent(project_id=GCP_PROJECT_ID, location=GCP_LOCATION, model_name=GEMINI_MODEL_NAME)
crime_analyzer = CrimeAnalyzerAgent(project_id=GCP_PROJECT_ID, location=GCP_LOCATION, model_name=GEMINI_MODEL_NAME)

@app.route('/', methods=['POST'])
async def index():
    """
    Receives Pub/Sub messages, processes them with AI agents,
    and publishes the results.
    """
    envelope = request.get_json()
    if not envelope:
        print("No Pub/Sub message received.")
        return ('Bad Request: no Pub/Sub message received', 400)

    if not isinstance(envelope, dict) or 'message' not in envelope:
        print(f"Invalid Pub/Sub message format: {envelope}")
        return ('Bad Request: invalid Pub/Sub message format', 400)

    pubsub_message = envelope['message']

    if 'data' not in pubsub_message:
        print("No data in Pub/Sub message.")
        return ('Bad Request: no data in Pub/Sub message', 400)

    try:
        # Decode the base64 encoded data
        data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        incident_data = json.loads(data)
        print(f"Received incident for processing: {incident_data}")

        # Extract incident description
        incident_description = incident_data.get('description', '')
        incident_location = incident_data.get('location', {})
        original_report_id = incident_data.get('originalReportId')
        firestore_doc_id = incident_data.get('firestoreDocId')

        if not incident_description:
            print("Incident description is missing.")
            return ('Bad Request: incident description missing', 400)

        # Run AI analyses concurrently
        incident_insights, mood_insights, crime_insights = await asyncio.gather(
            incident_analyzer.analyze(incident_description, incident_location),
            mood_analyzer.analyze(incident_description),
            crime_analyzer.analyze(incident_description)
        )

        # Consolidate insights
        consolidated_insights = {
            "originalReportId": original_report_id,
            "firestoreDocId": firestore_doc_id,
            "description": incident_description,
            "location": incident_location,
            "mediaUrls": incident_data.get('mediaUrls', []),
            "ingestedAt": incident_data.get('ingestedAt'),
            "aiProcessedAt": firestore.SERVER_TIMESTAMP, # Use server timestamp for consistency
            "status": "AI_Analyzed",
            "incidentAnalysis": incident_insights,
            "moodAnalysis": mood_insights,
            "crimeAnalysis": crime_insights
        }

        # Publish consolidated insights to analytics-and-suggestions topic
        topic_path = publisher.topic_path(GCP_PROJECT_ID, ANALYTICS_SUGGESTIONS_TOPIC_NAME)
        future = publisher.publish(topic_path, json.dumps(consolidated_insights).encode('utf-8'))
        message_id = future.result()
        print(f"Published consolidated insights with ID: {message_id}")

        return ('', 204) # 204 No Content for successful processing

    except Exception as e:
        print(f"Error processing Pub/Sub message: {e}")
        import traceback
        traceback.print_exc() # Print full traceback to logs
        return (f'Error: {e}', 500)

# Wrap the Flask app with WSGI2ASGI to make it compatible with Uvicorn
asgi_app = WSGI2ASGI(app) # <--- ADD THIS LINE

# This is for local development only. Cloud Run uses Gunicorn to run the app.
if __name__ == '__main__':
    # Use host '0.0.0.0' to make the server accessible externally
    app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 8080))

