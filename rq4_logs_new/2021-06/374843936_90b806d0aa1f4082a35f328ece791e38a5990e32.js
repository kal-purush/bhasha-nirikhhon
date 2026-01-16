import firebase from 'firebase/app';
import 'firebase/firestore';
import 'firebase/auth';

const config = {
    apiKey: "AIzaSyD_dAg9tnkub8ELz6H8L2Kk-vPC-vUCHow",
    authDomain: "zcrwn-db.firebaseapp.com",
    projectId: "zcrwn-db",
    storageBucket: "zcrwn-db.appspot.com",
    messagingSenderId: "625523102091",
    appId: "1:625523102091:web:dbd153ac3d1cd6d241df8e",
    measurementId: "G-BRGBVH54TY"
  };

firebase.initializeApp(config);

export const auth = firebase.auth();
export const firestore = firebase.firestore();

const provider = new firebase.auth.GoogleAuthProvider();
provider.setCustomParameters({ prompt: 'select_account' });
export const signInWithGoogle = () => auth.signInWithPopup(provider);

export default firebase;