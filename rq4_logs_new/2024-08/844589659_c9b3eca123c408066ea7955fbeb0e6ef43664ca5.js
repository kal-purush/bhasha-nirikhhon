// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";
import { getStorage } from "firebase/storage";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyAti14CS4NgXXGgBdsCUm0aL2oU4dGloLE",
  authDomain: "inventory-tracker-faec6.firebaseapp.com",
  projectId: "inventory-tracker-faec6",
  storageBucket: "inventory-tracker-faec6.appspot.com",
  messagingSenderId: "529962005327",
  appId: "1:529962005327:web:b62112fe09314d8b920b3a",
  measurementId: "G-YFB9M3VJLF"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const firestore = getFirestore(app);
export { firestore };
export const storage = getStorage(app);