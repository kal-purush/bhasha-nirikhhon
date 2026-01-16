// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
import { getStorage } from "firebase/storage";
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
    apiKey: "AIzaSyB4XX_oMTneCNHLCkuguC00g0kozwvsGwg",
    authDomain: "bullpowr-app.firebaseapp.com",
    projectId: "bullpowr-app",
    storageBucket: "bullpowr-app.appspot.com",
    messagingSenderId: "562954567999",
    appId: "1:562954567999:web:76d01f834399dc48ae938a",
    measurementId: "G-SK6B8KB495"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);
const storage = getStorage(app);

export { storage, app }