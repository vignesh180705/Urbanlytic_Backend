import json
import logging
from .base_agent import BaseAgent
from typing import Dict

logger = logging.getLogger(__name__)

class CrimeAnalyzerAgent(BaseAgent):
    """
    Specialized agent for identifying potential crime-related elements in an incident report.
    """
    def __init__(self, project_id: str, location: str, model_name: str):
        super().__init__(project_id, location, model_name)
        logger.info("CrimeAnalyzerAgent initialized.")

    async def analyze(self, incident_data: Dict) -> Dict:
        """
        Analyzes the incident description for indications of crime.
        """
        description = incident_data.get('description', 'No description provided.')

        prompt = f"""
        Analyze the following incident report to determine if there are any indications of a crime or illegal activity.
        Return the response in JSON format with the following keys:
        - `isCrimeRelated`: boolean (true if crime is indicated, false otherwise).
        - `crimeType`: Specific type of crime if detected (e.g., "theft", "vandalism", "assault", "drug activity", "public disturbance", "N/A" if not crime related).
        - `potentialSubjects`: An array of descriptions of potential subjects involved (e.g., ["male, dark hoodie"], ["group of teenagers"], "N/A" if none).
        - `requiresPoliceIntervention`: boolean (true if police intervention seems necessary based on the report).

        Incident Report:
        "{description}"

        Ensure the output is strictly JSON. Example:
        ```json
        {{
          "isCrimeRelated": true,
          "crimeType": "vandalism",
          "potentialSubjects": ["group of teenagers"],
          "requiresPoliceIntervention": true
        }}
        ```
        """
        
        logger.info(f"Calling Gemini for Crime Analysis with prompt snippet: {prompt[:100]}...")
        raw_ai_response = await self._call_gemini(prompt)
        ai_insights = self._parse_gemini_json_output(raw_ai_response)
        logger.info(f"Received Crime Analysis insights: {ai_insights}")
        return ai_insights
