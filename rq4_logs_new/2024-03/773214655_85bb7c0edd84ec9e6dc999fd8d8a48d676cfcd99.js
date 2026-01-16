// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyCIIjKrHkHBdtPFFXZaqtW7rG-2MtuFz28",
  authDomain: "emajon-lite-website.firebaseapp.com",
  projectId: "emajon-lite-website",
  storageBucket: "emajon-lite-website.appspot.com",
  messagingSenderId: "1039849878045",
  appId: "1:1039849878045:web:d541aef70c09cdab54ccf4"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export default app;