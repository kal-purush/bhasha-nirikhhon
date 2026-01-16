import firebase from "firebase/compat/app";
import "firebase/compat/auth";
import "firebase/compat/firestore";

const firebaseConfig = {
  apiKey: "AIzaSyBlwPOZGvVl3jZOq2pism3v2_RWXeR2-jA",
  authDomain: "tubes-pam-2a386.firebaseapp.com",
  projectId: "tubes-pam-2a386",
  storageBucket: "tubes-pam-2a386.appspot.com",
  messagingSenderId: "929913570272",
  appId: "1:929913570272:web:3d7558d49a9cebc533e185",
  measurementId: "G-4LNNSHMZVY",
};

if (!firebase.apps.length) {
  firebase.initializeApp(firebaseConfig);
}

export { firebase };