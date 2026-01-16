import { initializeApp } from "firebase/app";

const firebaseConfig = {
  apiKey: "AIzaSyB5N81DpUsQmYs-lEZwbt6QTtljKkrr-po",
  authDomain: "personal-portfolio-b6ac1.firebaseapp.com",
  databaseURL:
    "https://personal-portfolio-b6ac1-default-rtdb.europe-west1.firebasedatabase.app",
  projectId: "personal-portfolio-b6ac1",
  storageBucket: "personal-portfolio-b6ac1.appspot.com",
  messagingSenderId: "911096484471",
  appId: "1:911096484471:web:b529471ca9db6709abec72",
};

export const app = initializeApp(firebaseConfig);