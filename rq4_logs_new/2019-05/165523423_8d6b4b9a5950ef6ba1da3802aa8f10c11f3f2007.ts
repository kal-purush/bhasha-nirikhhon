import Firebase from 'firebase/app';
import 'firebase/auth';
import 'firebase/database';

// config file required to initialize Firebase app, values injected based on .ev file at runtime
const config = {
  apiKey: process.env.REACT_APP_API_KEY,
  authDomain: process.env.REACT_APP_AUTH_DOMAIN,
  databaseURL: process.env.REACT_APP_DATABASE_URL,
  projectId: process.env.REACT_APP_PROJECT_ID,
  storageBucket: process.env.REACT_APP_STORAGE_BUCKET,
  messagingSenderId: process.env.REACT_APP_MESSAGING_SENDER_ID
};

// intialize Firebase app
Firebase.initializeApp(config);

// access to auth
export const Auth = Firebase.auth();

// access to real-time database
export const DB = Firebase.database();