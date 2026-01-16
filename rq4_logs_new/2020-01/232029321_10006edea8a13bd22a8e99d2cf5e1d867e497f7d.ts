// Firebase App (the core Firebase SDK) is always required and must be listed first
import * as firebase from "firebase/app";

// Add the Firebase products that you want to use
import "firebase/auth";
import "firebase/firestore";

const firebaseConfig = {
  apiKey: "AIzaSyAZXpkQlAX61C5MlxK4BX4vU3bctIwHpzI",
  authDomain: "rejectionboard.firebaseapp.com",
  databaseURL: "https://rejectionboard.firebaseio.com",
  projectId: "rejectionboard",
  storageBucket: "rejectionboard.appspot.com",
  messagingSenderId: "144181020633",
  appId: "1:144181020633:web:616e8238dc8eb3ffabfbc8",
  measurementId: "G-1539N163E6"
};

firebase.initializeApp(firebaseConfig);

export default firebase;