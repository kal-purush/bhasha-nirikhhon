// Import the functions you need from the SDKs you need
import { initializeApp, getApp, getApps } from "firebase/app";
import { getFirestore } from "firebase/firestore";
import { getAuth } from "firebase/auth";

// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyA0vtcsj67ldyKoxTjawvlXuA8-W9tfKTM",
  authDomain: "netflix-clone-9ac2e.firebaseapp.com",
  projectId: "netflix-clone-9ac2e",
  storageBucket: "netflix-clone-9ac2e.appspot.com",
  messagingSenderId: "40614484472",
  appId: "1:40614484472:web:5145b94318b7b2c0601e0d",
};

// Initialize Firebase
const app = !getApps().length ? initializeApp(firebaseConfig) : getApp();
const db = getFirestore();
const auth = getAuth();

export default app;
export { auth, db };