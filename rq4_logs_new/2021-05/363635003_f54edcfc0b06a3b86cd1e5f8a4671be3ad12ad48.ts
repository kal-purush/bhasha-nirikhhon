// ESLint is too dumb to understand that this dependency
// is already included. This is why TSLint is better
// but we can't have nice things.
/* eslint-disable import/no-extraneous-dependencies */
import firebase from 'firebase/app';

import 'firebase/firestore';
/* eslint-enable import/no-extraneous-dependencies */

export function initFirebase() {
  if (firebase.apps.length === 0) {
    return firebase.initializeApp({
      apiKey: FIREBASE_API_KEY,
      authDomain: `${FIREBASE_PROJECT_ID}.firebaseapp.com`,
      databaseURL: `https://${FIREBASE_PROJECT_ID}.firebaseio.com`,
      projectId: `${FIREBASE_PROJECT_ID}`,
      storageBucket: `${FIREBASE_PROJECT_ID}.appspot.com`,
      messagingSenderId: FIREBASE_MESSAGING_ID,
      appId: FIREBASE_APP_ID,
    });
  } else {
    return firebase.app();
  }
}

export function getFirestore() {
  const db = firebase.firestore();
  return db;
}

export function getFirestoreCollection(collectionName: string) {
  const db = firebase.firestore();
  return db.collection(collectionName);
}