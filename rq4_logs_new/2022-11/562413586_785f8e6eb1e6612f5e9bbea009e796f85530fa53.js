import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";
import { getAuth } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyCQrkwFM-_3Ar9Y6KDKwXQ5RVy67-eqVzU",
  authDomain: "noteballs-b4482.firebaseapp.com",
  projectId: "noteballs-b4482",
  storageBucket: "noteballs-b4482.appspot.com",
  messagingSenderId: "630945479400",
  appId: "1:630945479400:web:5bc4a52838ed044748275d",
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);
const auth = getAuth(app);

export { db, auth };