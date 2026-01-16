import { initializeApp } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: process.env.REACT_APP_API_KEY,
  authDomain: process.env.REACT_APP_AUTH_DOMAIN,
  projectId: process.env.REACT_APP_PROJECT_ID,
  storageBucket: process.env.REACT_APP_STORAGE_BUCKET,
  messagingSenderId: process.env.REACT_APP_MESSAGING_SENDER_ID,
  appId: process.env.REACT_APP_APP_ID
};

if (!firebaseConfig.apiKey)
  throw new Error('Missing firebase credential: apiKey');
if (!firebaseConfig.authDomain)
  throw new Error('Missing firebase credential: authDomain');
if (!firebaseConfig.projectId)
  throw new Error('Missing firebase credential: projectId');
if (!firebaseConfig.storageBucket)
  throw new Error('Missing firebase credential: storageBucket');
if (!firebaseConfig.messagingSenderId)
  throw new Error('Missing firebase credential: messagingSenderId');
if (!firebaseConfig.appId)
  throw new Error('Missing firebase credential: appId');

// init firebase app
const app = initializeApp(firebaseConfig);

// init services
export const db = getFirestore(app);