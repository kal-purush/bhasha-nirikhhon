import firebase from 'firebase/compat/app';
import 'firebase/compat/firestore';
import dotenv from "dotenv";

dotenv.config();

const firebaseConfig = {
  apiKey: process.env.APIKEY,
  authDomain: process.env.DOMAIN,
  projectId: process.env.ID,
  storageBucket: process.env.STORAGE,
  messagingSenderId: process.env.MESSAGEID,
  appId: process.env.APPID,
  measurementId: process.env.MEASUREMENTID
};

firebase.initializeApp(firebaseConfig);
const db = firebase.firestore();

db.collection("test")
  .get()
  .then(() => console.log("Conexión exitosa con Firebase Firestore"))
  .catch((error) => console.error("Error al conectarse a Firebase Firestore"));

export { db };