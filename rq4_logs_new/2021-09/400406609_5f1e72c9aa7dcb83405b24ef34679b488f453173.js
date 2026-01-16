// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyD1H5Oe8JacH_wB9gpb42-5oU1f3wIIkKo",
  authDomain: "pranya-designs.firebaseapp.com",
  projectId: "pranya-designs",
  storageBucket: "pranya-designs.appspot.com",
  messagingSenderId: "472097076343",
  appId: "1:472097076343:web:660dd6bee2798813f30272",
  measurementId: "G-RW6BV8E7BV"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);