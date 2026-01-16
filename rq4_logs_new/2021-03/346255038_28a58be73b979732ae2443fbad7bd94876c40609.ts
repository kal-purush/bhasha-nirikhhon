import Index from "./Index.svelte";

import firebase from "firebase/app";

import "firebase/auth";
import "firebase/database";

firebase.initializeApp({
  apiKey: "AIzaSyAmsgiaooSt4eoC2Bh-nTxpfCf_yqOD99w",
  authDomain: "finnomena-port.firebaseapp.com",
  databaseURL: "https://finnomena-port-default-rtdb.europe-west1.firebasedatabase.app",
  projectId: "finnomena-port",
  storageBucket: "finnomena-port.appspot.com",
  messagingSenderId: "28883396382",
  appId: "1:28883396382:web:d75c0d951c06c39bb88b0f"
});

const index = new Index({
  target: document.body,
  props: {
    firebase
  }
});

export default index;