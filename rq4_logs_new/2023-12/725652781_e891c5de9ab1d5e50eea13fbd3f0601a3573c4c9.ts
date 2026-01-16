// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
import { getFirestore } from "firebase/firestore";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyCp3PlfxUF-ajJuTMEjQYXERocl71slqMQ",
  authDomain: "ecommerce-nizam.firebaseapp.com",
  projectId: "ecommerce-nizam",
  storageBucket: "ecommerce-nizam.appspot.com",
  messagingSenderId: "356015600359",
  appId: "1:356015600359:web:998e43ff6d4eeaf10fa842",
  measurementId: "G-5L09C5PDNL"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const db = getFirestore(app)