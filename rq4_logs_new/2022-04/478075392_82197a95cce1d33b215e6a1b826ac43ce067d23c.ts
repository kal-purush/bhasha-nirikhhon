// Import the functions you need from the SDKs you need
import { initializeApp} from "firebase/app";
import { getFirestore } from 'firebase/firestore';
import { getAuth } from "firebase/auth";


// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyDx5qyIOuyMQORZg1zxo1RbHyqH5B6SRdU",
  authDomain: "finalproject-5ce83.firebaseapp.com",
  projectId: "finalproject-5ce83",
  storageBucket: "finalproject-5ce83.appspot.com",
  messagingSenderId: "10252150468",
  appId: "1:10252150468:web:52a28e2c722e01ee290b91"
};

// Initialize Firebase
export const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
export const auth = getAuth(app);