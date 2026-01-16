import { initializeApp } from "firebase/app";
import { getFirestore } from 'firebase/firestore/lite';

const firebaseConfig = {
    apiKey: "AIzaSyA45EGeGUXvah6ByJkAO4HZNLkOARSb3-c",
    authDomain: "planos-1fa34.firebaseapp.com",
    projectId: "planos-1fa34",
    storageBucket: "planos-1fa34.appspot.com",
    messagingSenderId: "40901276661",
    appId: "1:40901276661:web:9e4cf01980e3ed1fcae6bc",
    measurementId: "G-GCKGR7JCMB"
  };
  
  // Initialize Firebase
  const firebaseApp = initializeApp(firebaseConfig);
  const db = getFirestore(firebaseApp);
  
  export default db;