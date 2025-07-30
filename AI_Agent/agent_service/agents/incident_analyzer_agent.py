import json
import logging
from .base_agent import BaseAgent
from typing import Dict

logger = logging.getLogger(__name__)

class IncidentAnalyzerAgent(BaseAgent):
    """
    Specialized agent for analyzing core incident details:
    event type, summary, and predicted impact.
    """
    def __init__(self, project_id: str, location: str, model_name: str):
        super().__init__(project_id, location, model_name)
        logger.info("IncidentAnalyzerAgent initialized.")

    async def analyze(self, incident_data: Dict) -> Dict:
        """
        Analyzes the incident description to extract event type, summary, and predicted impact.
        """
        description = incident_data.get('description', 'No description provided.')
        location_info = incident_data.get('location')
        
        location_str = f"Location: Lat {location_info['latitude']}, Lng {location_info['longitude']}" if location_info else "Location: N/A"

        prompt = f"""
        Analyze the following incident report and extract the following information in JSON format:
        - `eventType`: Categorize the incident (e.g., "traffic", "safety", "infrastructure", "road_hazard", "accident", "pothole", "public_disturbance", "waste_management", "environmental"). Choose the most relevant single category.
        - `summary`: A concise, 1-2 sentence summary of the incident.
        - `predictedImpact`: An object with the following keys:
            - `duration`: Estimated duration of the impact (e.g., "2 hours", "until cleared", "1-2 days").
            - `affectedCommuters`: Estimated number of people affected (integer, 0 if N/A).
            - `spreadDirection`: Direction of impact if applicable (e.g., "Northbound", "Local area", "N/A").
            - `severity`: Overall severity ("low", "medium", "high").

        Incident Report:
        Description: "{description}"
        {location_str}

        Ensure the output is strictly JSON. Example:
        ```json
        {{
          "eventType": "traffic",
          "summary": "A multi-car pile-up has blocked all lanes on Main Street near the city center. Emergency services are on the scene.",
          "predictedImpact": {{
            "duration": "2 hours",
            "affectedCommuters": 5000,
            "spreadDirection": "Northbound",
            "severity": "high"
          }}
        }}
        ```
        """
        
        logger.info(f"Calling Gemini for Incident Analysis with prompt snippet: {prompt[:100]}...")
        raw_ai_response = await self._call_gemini(prompt)
        ai_insights = self._parse_gemini_json_output(raw_ai_response)
        logger.info(f"Received Incident Analysis insights: {ai_insights}")
        return ai_insights
