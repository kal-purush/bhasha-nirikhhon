import { initializeApp } from 'firebase/app';
import { getAuth, signInWithEmailAndPassword } from 'firebase/auth';
import { getStorage } from 'firebase/storage';

const firebaseConfig = {
  apiKey: 'variable_api_key',

  authDomain: 'realtime-database-prototype.firebaseapp.com',

  databaseURL: 'https://realtime-database-prototype-default-rtdb.europe-west1.firebasedatabase.app',

  projectId: 'realtime-database-prototype',

  storageBucket: 'realtime-database-prototype.appspot.com',

  messagingSenderId: 'variable_sender_id',

  appId: 'variable_app_id',
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const storage = getStorage(app);

export { auth, signInWithEmailAndPassword, storage };