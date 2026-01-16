// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyANkbEAgGvxye0_lq7Q7h6qHaEIR93G-zM",
  authDomain: "react-twitter-practice.firebaseapp.com",
  projectId: "react-twitter-practice",
  storageBucket: "react-twitter-practice.appspot.com",
  messagingSenderId: "1066354140241",
  appId: "1:1066354140241:web:6b7841d0b5e02076004470"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

// Initialize Cloud Firestore and get a reference to the service
const db = getFirestore(app);

//エクスポートすることで全てのファイルでimportが可能になる
export default db;