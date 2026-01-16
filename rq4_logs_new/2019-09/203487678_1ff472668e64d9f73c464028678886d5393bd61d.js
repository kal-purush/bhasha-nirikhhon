import firebase from "firebase/app";
import "firebase/firestore";
import "firebase/auth";

const config = {
  apiKey: "AIzaSyCzoivXGUi5tE8m3EHYfITGWPrWZp62_3Q",
  authDomain: "crwn-db-964ad.firebaseapp.com",
  databaseURL: "https://crwn-db-964ad.firebaseio.com",
  projectId: "crwn-db-964ad",
  storageBucket: "",
  messagingSenderId: "571995352858",
  appId: "1:571995352858:web:8e9e206c91f0f61f9d06a3"
};

firebase.initializeApp(config);

export const auth = firebase.auth();
export const firestore = firebase.firestore();

const provider = new firebase.auth.GoogleAuthProvider();
provider.setCustomParameters({ prompt: "select_account" });
export const signInWithGoogle = () => auth.signInWithPopup(provider);

export default firebase;