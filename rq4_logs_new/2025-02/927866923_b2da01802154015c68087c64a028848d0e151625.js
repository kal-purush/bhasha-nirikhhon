/**
 * Import function triggers from their respective submodules:
 *
 * const {onCall} = require("firebase-functions/v2/https");
 * const {onDocumentWritten} = require("firebase-functions/v2/firestore");
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

// Cloud functions for Firebase SDK to create Cloud Functions and triggers
import {onRequest} from "firebase-functions/v2/https";
import * as logger from "firebase-functions/logger";
import { onDocumentCreated } from "firebase-functions/v2/firestore";

// The firebase Admin SDK to access Firestore.
import { initializeApp } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

import * as functions from "firebase-functions";
import * as admin from "firebase-admin";

initializeApp();

export const addMessage = onRequest(async (req, res) => {
    const original = req.query.text;
    const writeResult = await getFirestore()
        .collection('messages')
        .add({original: original});

    res.json({result: `Message with ID: ${writeResult.id} added.`});
})

export const makeUppercase = onDocumentCreated("/messages/{documentId}", (event) => {
    const original = event.data?.data()?.original;

    logger.log("Uppercasing", event.params.documentId, original);
    const uppercase = original.toUpperCase();

    return event.data?.ref.set({uppercase}, {merge: true});
})

// Start writing functions
// https://firebase.google.com/docs/functions/typescript

export const helloWorld = functions.https.onRequest((request, response) => {
  response.send("Hello from Firebase!");
});

// Create and deploy your first functions
// https://firebase.google.com/docs/functions/get-started

// exports.helloWorld = onRequest((request, response) => {
//   logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });