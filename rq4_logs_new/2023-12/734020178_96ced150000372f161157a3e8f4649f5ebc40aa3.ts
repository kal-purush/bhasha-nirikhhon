import { getApp, getApps, initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore } from "firebase/firestore";

const firebaseConfig = {
  apiKey: "AIzaSyAf0oi4SND63eUClsUgmtCr7u_ApAAyN4s",
  authDomain: "gif-hub-auth.firebaseapp.com",
  projectId: "gif-hub-auth",
  storageBucket: "gif-hub-auth.appspot.com",
  messagingSenderId: "1034736357497",
  appId: "1:1034736357497:web:af7f431012ddbee5a144a8"
};

// Initialize Firebase
const app = getApps().length ? getApp() : initializeApp(firebaseConfig);
const db = getFirestore();
const auth = getAuth();

export { app, db, auth };