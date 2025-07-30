import base64
import json

# Paste your JSON here
pre_processed_report_json = {
    "id": "test-report-001",
    "description": "There's a huge pothole on Main Street near the central park entrance. It's causing traffic delays and looks dangerous. People are really upset about it.",
    "location": {
        "latitude": 13.0674,
        "longitude": 80.2376
    },
    "type": "Road Hazard",
    "mediaUrls": ["https://example.com/pothole_image.jpg"],
    "ingestedAt": "2025-07-22T10:00:00Z",
    "firestoreDocId": "someFirestoreDocId123"
}

# Encode to base64
encoded_data = base64.b64encode(json.dumps(pre_processed_report_json).encode('utf-8')).decode('utf-8')
print(encoded_data)
