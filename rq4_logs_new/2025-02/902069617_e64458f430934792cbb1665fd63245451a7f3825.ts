// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyDF0LnvGfbLgZAKRM0PhCvGDjP9H924VXg",
  authDomain: "delivo-testbot-82f20.firebaseapp.com",
  projectId: "delivo-testbot-82f20",
  storageBucket: "delivo-testbot-82f20.firebasestorage.app",
  messagingSenderId: "154158723086",
  appId: "1:154158723086:web:38774003f17492ffd14c5f",
  measurementId: "G-D69VJFRF3W"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);