import json
import logging
from .base_agent import BaseAgent
from typing import Dict

logger = logging.getLogger(__name__)

class MoodAnalyzerAgent(BaseAgent):
    """
    Specialized agent for analyzing the mood/sentiment of the incident report.
    """
    def __init__(self, project_id: str, location: str, model_name: str):
        super().__init__(project_id, location, model_name)
        logger.info("MoodAnalyzerAgent initialized.")

    async def analyze(self, incident_data: Dict) -> Dict:
        """
        Analyzes the incident description for sentiment and mood.
        """
        description = incident_data.get('description', 'No description provided.')

        prompt = f"""
        Analyze the sentiment and overall mood expressed in the following incident report.
        Return the response in JSON format with the following keys:
        - `overallSentiment`: Categorize as "positive", "neutral", "negative", or "mixed".
        - `emotionsDetected`: An array of specific emotions detected (e.g., "anger", "frustration", "concern", "calm", "joy"). Empty array if none.
        - `sentimentScore`: A numerical score from -1.0 (very negative) to 1.0 (very positive), 0.0 for neutral.

        Incident Report:
        "{description}"

        Ensure the output is strictly JSON. Example:
        ```json
        {{
          "overallSentiment": "negative",
          "emotionsDetected": ["frustration", "concern"],
          "sentimentScore": -0.7
        }}
        ```
        """
        
        logger.info(f"Calling Gemini for Mood Analysis with prompt snippet: {prompt[:100]}...")
        raw_ai_response = await self._call_gemini(prompt)
        ai_insights = self._parse_gemini_json_output(raw_ai_response)
        logger.info(f"Received Mood Analysis insights: {ai_insights}")
        return ai_insights
