// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyBbDN6txVIeN-H6iO6AljxLZ7OftQsVMZQ",
  authDomain: "lb-jsi.firebaseapp.com",
  databaseURL: "https://lb-jsi-default-rtdb.firebaseio.com",
  projectId: "lb-jsi",
  storageBucket: "lb-jsi.appspot.com",
  messagingSenderId: "1014450536482",
  appId: "1:1014450536482:web:a61c655d95f9bdd1b7ca7b",
  measurementId: "G-4DG40YNBH9"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);