import * as admin from 'firebase-admin';

// firebase project ID
const PROJ_ID = 'slack-in-your-time';

// initialize firebase in order to access its services
// note: the env var for `GOOGLE_APPLICATION_CREDENTIALS` must be set (see: https://firebase.google.com/docs/admin/setup#initialize-sdk)
const firebaseApp = admin.initializeApp({
    credential: admin.credential.applicationDefault(),
    databaseURL: `https://${PROJ_ID}.firebaseio.com`,
});

// initialize the database and collections
const db = firebaseApp.firestore();
// database settings
db.settings({ ignoreUndefinedProperties: true });

// export them so it can be globally accessed
export { admin, db };