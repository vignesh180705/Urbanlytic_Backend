const express = require('express');
const { PubSub } = require('@google-cloud/pubsub');
const admin = require('firebase-admin'); // Import firebase-admin
const cors = require('cors');           // Import cors middleware

const app = express();
const pubsub = new PubSub();

// Initialize Firebase Admin SDK
// Cloud Run services running in the same GCP project as Firestore will automatically
// find the default credentials.
admin.initializeApp();
const db = admin.firestore(); // Initialize Firestore

// ----------------------------------------------------
// CORS Configuration
// This enables CORS for all routes. For production, you might want to restrict
// origin to your frontend's domain (e.g., origin: 'https://your-frontend-domain.com')
app.use(cors({
  origin: '*', // Allow all origins for now. Restrict this in production.
  methods: 'GET,HEAD,PUT,PATCH,POST,DELETE', // Allow the methods your service uses
  credentials: true, // Allow cookies to be sent
  optionsSuccessStatus: 204 // Respond with 204 for successful preflight
}));
// ----------------------------------------------------

// Middleware to parse JSON request bodies
app.use(express.json());

// Define the Pub/Sub topic name
const TOPIC_NAME = 'raw-user-reports'; // This must match the Topic ID you created earlier

/**
 * HTTP POST endpoint for receiving user incident reports.
 * - Stores the raw data in a new 'rawReports' Firestore collection.
 * - Validates the incoming data.
 * - Publishes the incident data (with Firestore ID) to the 'raw-user-reports' Pub/Sub topic.
 */
app.post('/ingest', async (req, res) => {
  const incidentData = req.body;

  // 1. Basic Input Validation
  // Ensure 'reportedBy' is also validated, as this was a key missing piece.
  if (!incidentData || !incidentData.description || !incidentData.location || !incidentData.reportedBy) {
    console.error('Validation failed: Missing required fields in incident data.', incidentData);
    return res.status(400).json({ error: 'Bad Request: Missing required incident data (description, location, or reportedBy).' });
  }

  // Add a server-side timestamp and initial status for the raw report
  incidentData.ingestedAt = admin.firestore.FieldValue.serverTimestamp();
  incidentData.status = 'raw_ingested';

  let firestoreDocId; // To store the Firestore document ID

  try {
    // 2. Store Raw Data in Firestore FIRST
    // Use a new collection like 'rawReports' for the original, untouched data.
    const docRef = await db.collection('rawReports').add(incidentData);
    firestoreDocId = docRef.id;
    console.log(`Raw incident written to Firestore with ID: ${firestoreDocId}`);

    // 3. Prepare data for Pub/Sub (optional: include Firestore doc ID)
    // Adding the firestoreDocId allows downstream services to link back to the raw record.
    const dataToPublish = { ...incidentData, firestoreDocId: firestoreDocId };
    const dataBuffer = Buffer.from(JSON.stringify(dataToPublish));

    // 4. Publish Message to Pub/Sub
    const messageId = await pubsub.topic(TOPIC_NAME).publishMessage({ data: dataBuffer });

    console.log(`Successfully published message ${messageId} to topic ${TOPIC_NAME}. Firestore Doc ID: ${firestoreDocId}`);

    // 5. Send Success Response
    res.status(200).json({
      message: 'Incident received, stored, and queued for processing.',
      messageId: messageId,
      firestoreDocId: firestoreDocId, // Return the Firestore ID
      receivedTimestamp: new Date().toISOString()
    });

  } catch (error) {
    // 6. Handle Errors
    console.error(`Error processing or publishing message: ${error.message}`, error);
    res.status(500).json({ error: `Internal Server Error: Failed to ingest incident. Details: ${error.message}` });
  }
});

// Cloud Run provides a PORT environment variable. Your application must listen on this port.
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Data Ingestor Cloud Run service listening on port ${port}`);
});