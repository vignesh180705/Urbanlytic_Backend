# agents/base_agent.py (FIXED - Using GenerativeModel)
import json
import logging
from abc import ABC, abstractmethod
# Use the higher-level GenerativeModel client
from vertexai.preview.generative_models import GenerativeModel, Part, Content
from google.api_core.exceptions import GoogleAPIError, InvalidArgument, ResourceExhausted # Added specific exceptions
import vertexai # Import vertexai for initialization

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """
    Base class for all specialized AI agents.
    Handles common Gemini API initialization and interaction using GenerativeModel.
    """
    def __init__(self, project_id: str, location: str, model_name: str):
        self.project_id = project_id
        self.location = location
        self.model_name = model_name
        self._initialize_vertex_ai() # Initialize Vertex AI
        self.model = self._get_generative_model() # Get the GenerativeModel instance
        logger.info(f"Initialized BaseAgent for model: {self.model_name} in {self.location}")

    def _initialize_vertex_ai(self):
        """Initializes Vertex AI with project and location."""
        try:
            vertexai.init(project=self.project_id, location=self.location)
            logger.info(f"Vertex AI initialized for project {self.project_id} in {self.location}")
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI: {e}")
            raise

    def _get_generative_model(self):
        """Gets the GenerativeModel instance."""
        try:
            return GenerativeModel(self.model_name)
        except Exception as e:
            logger.error(f"Failed to get GenerativeModel {self.model_name}: {e}")
            raise

    async def _call_gemini(self, prompt: str) -> str:
        """
        Makes an asynchronous call to the Gemini API using GenerativeModel.
        Returns the raw text response from the model.
        """
        try:
            # Use the higher-level generate_content method
            # It directly accepts a string prompt or a list of Content/Part objects
            response = await self.model.generate_content_async(prompt)
            
            # Access the text from the response
            if response.candidates:
                # Assuming the first candidate has the text content
                # The response object from generate_content_async is simpler to parse
                return response.candidates[0].text
            else:
                logger.warning("Gemini returned no candidates in the response.")
                return ""

        except (InvalidArgument, ResourceExhausted) as e:
            logger.error(f"Gemini API error (check prompt length/quotas): {e}")
            raise
        except GoogleAPIError as e:
            logger.error(f"Gemini API general error: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during Gemini call: {e}", exc_info=True)
            raise

    @abstractmethod
    async def analyze(self, incident_data: dict) -> dict:
        """
        Abstract method for specialized analysis.
        Each agent must implement this.
        """
        pass

    def _parse_gemini_json_output(self, raw_text: str) -> dict:
        """
        Parses the raw text response from Gemini, attempting to extract JSON.
        Removes markdown code blocks if present.
        """
        try:
            cleaned_text = raw_text.replace('```json', '').replace('```', '').strip()
            return json.loads(cleaned_text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from Gemini response: {e}. Raw text: {raw_text}")
            return {"error": "JSON parse error", "raw_response": raw_text}

