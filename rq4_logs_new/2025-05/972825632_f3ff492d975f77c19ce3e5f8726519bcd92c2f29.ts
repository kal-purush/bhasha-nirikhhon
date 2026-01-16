import { initializeApp, getApps } from "firebase/app";
import { getAuth } from "firebase/auth";
import { getFirestore } from "firebase/firestore";
import { getAnalytics } from "firebase/analytics";

const firebaseConfig = {
  apiKey: "AIzaSyBLXgy3COexvQRM6J794XKMEc2hUhQ4TCY",
  authDomain: "vethome-84011.firebaseapp.com",
  projectId: "vethome-84011",
  storageBucket: "vethome-84011.firebasestorage.app",
  messagingSenderId: "77410622492",
  appId: "1:77410622492:web:e5bc3a4da505f481ab53d9",
  measurementId: "G-DVDM5TGVMH"
};

// Initialize Firebase
const app = !getApps().length ? initializeApp(firebaseConfig) : getApps()[0];
const auth = getAuth(app);
const db = getFirestore(app);

// Initialize Analytics only in the browser
let analytics;
if (typeof window !== 'undefined') {
  analytics = getAnalytics(app);
}

export { app, auth, db, analytics }; 