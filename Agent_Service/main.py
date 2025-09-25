# main.py (Flask Version - Older)
import os
import json
import base64
import logging
from flask import Flask, request, jsonify
import firebase_admin # type: ignore
from firebase_admin import firestore # type: ignore

# Import Uvicorn's WSGIMiddleware
from uvicorn.middleware.wsgi import WSGIMiddleware

# Import Google Cloud clients and Vertex AI SDK components
from google.cloud import pubsub_v1 # For publishing messages
import vertexai
from vertexai.preview.generative_models import GenerativeModel
from google.api_core.exceptions import GoogleAPIError, InvalidArgument, ResourceExhausted
from datetime import datetime

# Import configuration variables (assuming config.py exists and is correct)
from config import (
    GCP_PROJECT_ID,
    GCP_LOCATION,
    PROCESSED_EVENTS_TOPIC_NAME,
    ANALYTICS_SUGGESTIONS_TOPIC_NAME,
    GEMINI_MODEL_NAME as CONFIG_GEMINI_MODEL_NAME
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Initialize Google Cloud Pub/Sub Publisher Client globally
publisher = pubsub_v1.PublisherClient()

# Set the Gemini model name
GEMINI_MODEL_NAME = CONFIG_GEMINI_MODEL_NAME  # Use config value for consistency

# Global variables for lazy initialization of GenerativeModel per worker process
_cached_gemini_model = None
_vertexai_initialized_flag = False

if not firebase_admin._apps:
    firebase_admin.initialize_app()


def update_specific_report(document_id, new_status, reason=None):
    """
    Finds a specific document by its ID in the UserReports collection
    and updates its status.

    Args:
        document_id (str): The unique ID of the document to update.
        new_status (str): The new status value (e.g., 'resolved', 'under_review').
    """
    try:
        db = firestore.client()
        
        # 1. Get a direct reference to the specific document using its ID
        doc_ref = db.collection('UserReports').document(document_id)

        # 2. Update the 'status' field with the new value
        logger.info(f"Attempting to update document: {document_id}")
        doc_ref.update({
            'status': new_status,
            'reason': reason if reason else ''
        })

        logger.info(f"Successfully updated status for report {document_id} to '{new_status}'")
        return f"Updated report {document_id}"

    except Exception as e:
        logger.info(f"An error occurred while updating {document_id}: {e}")
        return f"An error occurred: {e}"




def get_gemini_model_instance():
    """
    Lazily initializes Vertex AI and instantiates the GenerativeModel.
    Synchronous version for compatibility with Flask/Gunicorn.
    """
    global _cached_gemini_model, _vertexai_initialized_flag

    if not _vertexai_initialized_flag:
        try:
            # vertexai.init() is generally safe to call multiple times,
            # but we guard it to log only once per worker.
            vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)
            _vertexai_initialized_flag = True
            logger.info(f"Vertex AI initialized for project '{GCP_PROJECT_ID}' in location '{GCP_LOCATION}'.")
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI: {e}", exc_info=True)
            raise # Re-raise to indicate a critical startup failure

    if _cached_gemini_model is None:
        try:
            # Instantiate GenerativeModel. This might internally set up gRPC clients.
            _cached_gemini_model = GenerativeModel(GEMINI_MODEL_NAME)
            logger.info(f"GenerativeModel '{GEMINI_MODEL_NAME}' instantiated.")
        except Exception as e:
            logger.error(f"Failed to instantiate GenerativeModel '{GEMINI_MODEL_NAME}': {e}", exc_info=True)
            _cached_gemini_model = None # Reset on failure
            raise # Re-raise to indicate a critical startup failure

    return _cached_gemini_model


@app.post("/")
def index():
    """
    Receives Pub/Sub messages (or any POST request), logs it,
    makes a simple Gemini call, and publishes a simplified result.
    """
    try:
        start_time = datetime.utcnow()
        logger.info(f"Starting message processing at {start_time.isoformat()}")
        
        # Get the Gemini model instance (sync)
        gemini_model = get_gemini_model_instance()

        envelope = request.get_json()
        
        # Handle direct POST requests (for testing)
        if not envelope or 'message' not in envelope:
            logger.info("Received non-Pub/Sub POST request or malformed Pub/Sub envelope.")
            if envelope:
                logger.info(f"Request body: {json.dumps(envelope)}")
            
            # For direct testing, provide a dummy prompt and simplified output
            test_prompt = "What is the capital of France?"
            logger.info(f"Making a dummy Gemini call with: '{test_prompt}'")
            
            # Use the sync method
            gemini_response = gemini_model.generate_content(test_prompt)
            
            summary_from_gemini = gemini_response.text if hasattr(gemini_response, 'candidates') and gemini_response.candidates else "No summary generated."
            logger.info(f"Dummy Gemini response: {summary_from_gemini[:100]}...")

            simplified_output = {
                "status": "Test_Processed",
                "summary": summary_from_gemini,
                "source": "Direct_Test_Request"
            }
            return jsonify(simplified_output), 200

        # Process Pub/Sub message
        pubsub_message = envelope['message']
        message_id = pubsub_message.get('messageId', 'unknown')
        publish_time = pubsub_message.get('publishTime', 'unknown')
        logger.info(f"Received Pub/Sub message ID: {message_id}, publish time: {publish_time}")
        
        data = base64.b64decode(pubsub_message.get('data', '')).decode('utf-8')
        incident_data = json.loads(data)
        
        incident_id = incident_data.get('id', 'N/A')
        incident_type = incident_data.get('type', 'General Incident') 
        doc_id = incident_data.get('firestoreDocId')
        description_snippet = incident_data.get('description', '')[:50]
        logger.info(f"Processing incident ID: {incident_id} from message {message_id} - Content: {description_snippet}")
        
        # Make Gemini call synchronously with timeout handling
        prompt_for_gemini = f"""
        You are an expert complaint analyst. You will be given a description of an urban complaint.
        If the complaint is valid, provide a concise summary without changing the meaning.
        If it is not a valid complaint, say "Summary unavailable - insufficient data. Reason: <Whatever the reason is for discarding the complaint>".
        Summarize the following urban incident: {incident_data.get('description', 'No description provided.')}
        
        Example:
        Input: "There is a large pothole on 5th Avenue causing traffic delays."
        Output: "Large pothole on 5th Avenue causing traffic delays."

        Input: "Streetlight not working."
        Output: "Streetlight malfunction reported."

        Input: "No issues, just a routine check."
        Output: "Summary unavailable - insufficient data. Reason: Not a valid complaint."

        Input: "oquehfqnl"
        Output: "Summary unavailable - insufficient data. Reason: The Complaint has no valid information."
        """
        logger.info(f"Making Gemini call for incident {incident_id}")
        
        try:
            # Use concurrent.futures for timeout handling (works in threads)
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
            
            # Set a 30-second timeout for Gemini calls (well within 60-second ack deadline)
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(gemini_model.generate_content, prompt_for_gemini)
                gemini_response = future.result(timeout=30)
            
            summary_from_gemini = gemini_response.text if hasattr(gemini_response, 'candidates') and gemini_response.candidates else "No summary generated."
            logger.info(f"Received Gemini summary for {incident_id}: {summary_from_gemini[:100]}...")
            
        except FutureTimeoutError:
            logger.warning(f"Gemini call timed out for incident {incident_id}, using fallback summary")
            summary_from_gemini = f"Summary unavailable - processing timeout for incident: {incident_id}"
        except Exception as e:
            logger.error(f"Gemini call failed for incident {incident_id}: {e}")
            summary_from_gemini = f"Summary unavailable - processing error for incident: {incident_id}"
        
        # Prepare AI result
        simplified_ai_result = {
            "originalReportId": incident_id,
            "description": incident_data.get('description'),
            "aiGeneratedSummary": summary_from_gemini,
            "aiProcessedAt": datetime.utcnow().isoformat() + 'Z',
            "eventType": incident_type,
            "status": "Simplified_AI_Analyzed"
        }
        
        # Publish to Pub/Sub (synchronous) with timeout
        if not summary_from_gemini.startswith("Summary unavailable"):
            topic_path = publisher.topic_path(GCP_PROJECT_ID, ANALYTICS_SUGGESTIONS_TOPIC_NAME)
            message_data = json.dumps(simplified_ai_result).encode('utf-8')
            try:
                # Set a 10-second timeout for publishing (well within 60-second ack deadline)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(publisher.publish, topic_path, message_data)
                    pubsub_future = future.result(timeout=10)
                    published_message_id = pubsub_future.result()  # Get the actual message ID
                
                logger.info(f"Published AI result with ID: {published_message_id} for incident {incident_id}")
                logger.info(f"Input message ID: {message_id} -> Output message ID: {published_message_id}")
                
            except FutureTimeoutError:
                logger.error(f"Pub/Sub publishing timed out for incident {incident_id}")
                return 'Message Acknowledged (Publishing Timeout)', 200
            except Exception as e:
                logger.error(f"Pub/Sub publishing failed for incident {incident_id}: {e}")
                return 'Message Acknowledged (Publishing Error)', 200
            
            end_time = datetime.utcnow()
            processing_duration = (end_time - start_time).total_seconds()
            logger.info(f"Message processing completed in {processing_duration:.2f} seconds")
            
            return 'OK - Message Processed and AI Result Published (Simplified)', 200
        else:
            reason = summary_from_gemini.split("Reason:")[-1].strip() if "Reason:" in summary_from_gemini else "Unknown"
            logger.info(f"Gemini indicated to discard incident {incident_id}. Reason: {reason}")
            # Initialize the Firebase Admin SDK if not already done
            update_specific_report(doc_id,'Discarded',reason)
            logger.info(f"Skipping publishing for incident {incident_id} due to insufficient summary.   ")
            return 'OK - Message Processed but No Valid Summary to Publish', 200
    except (InvalidArgument, ResourceExhausted) as e:
        logger.error(f"Gemini API error: {e}", exc_info=True)
        # Return 200 OK for Pub/Sub to prevent retries on application errors
        return 'Internal Server Error: Gemini API Issue', 200
    except GoogleAPIError as e:
        logger.error(f"Gemini API general error: {e}", exc_info=True)
        return 'Internal Server Error: Gemini API General Issue', 200
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}", exc_info=True)
        # Return 200 OK for Pub/Sub to prevent retries, even on malformed messages
        return 'Message Acknowledged (JSON Error)', 200
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        # Return 200 OK for Pub/Sub to prevent retries
        return 'Message Acknowledged (Internal Error)', 200

# Wrap the Flask app with WSGIMiddleware to make it compatible with Uvicorn
asgi_app = WSGIMiddleware(app)

# For local development
if __name__ == '__main__':
    import uvicorn
    # Load environment variables for local run
    from dotenv import load_dotenv
    load_dotenv()
    
    port = int(os.environ.get('PORT', 8080))
    # Run the Flask app directly with Uvicorn
    uvicorn.run(asgi_app, host='0.0.0.0',port=port)