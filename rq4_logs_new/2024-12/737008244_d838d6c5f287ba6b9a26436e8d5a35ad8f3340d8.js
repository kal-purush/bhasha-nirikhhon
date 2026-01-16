// Import the functions you need from the Firebase SDK
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyDeoBAY-qnj2PCJdZrgByAGZIca0jiU7aM",
  authDomain: "ravinduwickramageportfolio.firebaseapp.com",
  projectId: "ravinduwickramageportfolio",
  storageBucket: "ravinduwickramageportfolio.firebasestorage.app",
  messagingSenderId: "79189462607",
  appId: "1:79189462607:web:1d1a005aaeb1ebb3ba243a",
  measurementId: "G-SC49GBN8S8",
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);

export { app, analytics };