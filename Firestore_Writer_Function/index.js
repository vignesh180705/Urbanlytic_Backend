const express = require('express');
const admin = require('firebase-admin');

// Initialize Firebase Admin SDK.
// In Cloud Run, this initializes automatically with credentials
// provided by the Cloud Run service account.
admin.initializeApp();

const app = express();
const db = admin.firestore(); // Get a reference to the Firestore database

// Middleware to parse JSON request bodies
app.use(express.json());

/**
 * HTTP POST endpoint for receiving Pub/Sub push messages from 'analytics-and-suggestions' topic.
 * This service writes the received (processed and AI-enriched) event data into Firestore.
 */
app.post('/', async (req, res) => {
  // 1. Validate Pub/Sub Message Format:
  // Pub/Sub push messages have a specific JSON structure.
  if (!req.body || !req.body.message || !req.body.message.data) {
    console.error('Invalid Pub/Sub push message format:', req.body);
    return res.status(400).send('Bad Request: Invalid Pub/Sub message format.');
  }

  // 2. Decode the Base64 Data:
  // The actual event data from Pub/Sub is base64 encoded.
  let eventData;
  try {
    const messageData = Buffer.from(req.body.message.data, 'base64').toString('utf8');
    eventData = JSON.parse(messageData);
    console.log('Received event for Firestore writing:', eventData);
  } catch (error) {
    console.error('Error decoding or parsing Pub/Sub message data:', error);
    return res.status(400).send('Bad Request: Could not decode or parse message data.');
  }

  // 3. Prepare data for Firestore:
  // Ensure location is converted to Firestore GeoPoint if available and valid.
  const { location, ...restOfData } = eventData;

  let firestoreLocation = null;
  if (location && typeof location.latitude === 'number' && typeof location.longitude === 'number') {
    firestoreLocation = new admin.firestore.GeoPoint(location.latitude, location.longitude);
  } else {
    console.warn('Location data missing or malformed for Firestore GeoPoint, saving as-is or null.', location);
    // If location is invalid for GeoPoint, store the original location data or null.
  }

  // Add a Firestore server timestamp for when the document was created.
  const dataToSave = {
    ...restOfData,
    location: firestoreLocation || (location || null), // Use GeoPoint if valid, else original loc or null
    firestoreCreatedAt: admin.firestore.FieldValue.serverTimestamp()
  };

  // 4. Write to Firestore:
  try {
    // Specify your Firestore collection (e.g., 'events').
    const docRef = await db.collection('events').add(dataToSave);
    console.log(`Document written with ID: ${docRef.id} to collection 'events'.`);

    // 5. Send Success Response: IMPORTANT for Pub/Sub push subscriptions.
    // A 2xx response tells Pub/Sub the message was successfully handled.
    res.status(200).send('Event written to Firestore.');

  } catch (error) {
    console.error('Error writing document to Firestore:', error);
    // Non-2xx response tells Pub/Sub to retry delivery.
    res.status(500).send('Internal Server Error: Failed to write event to Firestore.');
  }
});

// Cloud Run provides a PORT environment variable. Your application must listen on this port.
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Firestore Event Writer Cloud Run service listening on port ${port}`);
});