const express = require('express');
const { PubSub } = require('@google-cloud/pubsub');
const admin = require('firebase-admin'); // For admin.firestore.FieldValue.serverTimestamp()

// Initialize Firebase Admin SDK.
// In Cloud Run, this typically initializes automatically with default credentials
// provided by the Cloud Run service account.
admin.initializeApp();

const app = express();
const pubsub = new PubSub();

// Middleware to parse JSON request bodies
app.use(express.json());

// Define Pub/Sub topic names
const RAW_REPORTS_TOPIC = 'raw-user-reports'; // The topic this service consumes from (via push subscription)
const PROCESSED_EVENTS_TOPIC = 'processed-events'; // <--- UPDATED: The topic this service now publishes to

/**
 * HTTP POST endpoint for receiving Pub/Sub push messages.
 * Processes raw user reports and publishes them as processed events.
 */
app.post('/', async (req, res) => {
  // 1. Validate Pub/Sub Message Format:
  // Pub/Sub push messages arrive in a specific JSON format.
  if (!req.body || !req.body.message || !req.body.message.data) {
    console.error('Invalid Pub/Sub push message format:', req.body);
    return res.status(400).send('Bad Request: Invalid Pub/Sub message format.');
  }

  // 2. Decode the Base64 Data:
  // The actual Pub/Sub message content (your incident data) is base64 encoded.
  let incidentData;
  try {
    const messageData = Buffer.from(req.body.message.data, 'base64').toString('utf8');
    incidentData = JSON.parse(messageData);
    console.log('Received raw incident for processing:', incidentData);
  } catch (error) {
    console.error('Error decoding or parsing Pub/Sub message data:', error);
    return res.status(400).send('Bad Request: Could not decode or parse message data.');
  }

  // 3. Placeholder Processing Logic:
  // In a real scenario, this is where you'd integrate Gemini Vision/LLM models
  // to analyze the description, images (from URLs in incidentData), etc.
  // For now, we add a simple summary, status, and timestamp.
  const processedIncident = {
    ...incidentData,
    status: 'processed',
    // Using serverTimestamp for consistency if you eventually write this directly to Firestore in future steps
    processedTimestamp: admin.firestore.FieldValue.serverTimestamp(),
    eventType: incidentData.type || 'general_incident', // Use existing type or default
    summary: incidentData.description ? `Incident: ${incidentData.description.substring(0, Math.min(incidentData.description.length, 100))}...` : 'Incident reported.',
    // Add a default predictedImpact map if not present
    predictedImpact: incidentData.predictedImpact || {
      duration: "unknown",
      affectedCommuters: 0,
      spreadDirection: "N/A"
    },
    agentResponsible: "User Report Processing (Cloud Run Placeholder)"
  };

  // 4. Publish Processed Message to the 'processed-events' Topic:
  try {
    const dataBuffer = Buffer.from(JSON.stringify(processedIncident));
    const messageId = await pubsub.topic(PROCESSED_EVENTS_TOPIC).publishMessage({ data: dataBuffer }); // <--- Using PROCESSED_EVENTS_TOPIC
    console.log(`Processed message ${messageId} published to topic ${PROCESSED_EVENTS_TOPIC}.`);

    // 5. Send Success Response: IMPORTANT for Pub/Sub push subscriptions.
    // A 2xx response tells Pub/Sub the message was successfully handled.
    res.status(200).send('Message processed and published.');

  } catch (error) {
    console.error(`Error publishing processed message to ${PROCESSED_EVENTS_TOPIC}: ${error.message}`, error);
    // Non-2xx response tells Pub/Sub to retry delivery.
    res.status(500).send('Internal Server Error: Failed to publish processed message.');
  }
});

// Cloud Run provides a PORT environment variable. Your application must listen on this port.
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`User Report Processor Cloud Run service listening on port ${port}`);
});