// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getStorage } from "firebase/storage";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyAC6JZBP2zVmQHfeM7JXdgpKpO--vlKOtw",
  authDomain: "helpcife-f8af9.firebaseapp.com",
  projectId: "helpcife-f8af9",
  storageBucket: "helpcife-f8af9.appspot.com",
  messagingSenderId: "642276957498",
  appId: "1:642276957498:web:c82ff1fc7465d3652cc403"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const storage = getStorage(app);