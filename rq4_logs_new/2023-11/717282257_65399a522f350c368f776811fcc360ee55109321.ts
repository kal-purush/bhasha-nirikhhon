import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";
import { getAuth } from 'firebase/auth';

const firebaseConfig = {
    apiKey: "AIzaSyA8dgBddtgTMhl1xwuaFeUT2rnKk4831w0",
    authDomain: "phoster-668bc.firebaseapp.com",
    projectId: "phoster-668bc",
    storageBucket: "phoster-668bc.appspot.com",
    messagingSenderId: "1017660393789",
    appId: "1:1017660393789:web:19debabd0944f7d510dd54",
    measurementId: "G-LKE4B1MN7M"
};

const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
export const auth = getAuth(app);