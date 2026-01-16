import { initializeApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';

const firebaseConfig = {
  apiKey: "AIzaSyD8LnHikEqFGk1ehU47U0b7SNrsgowv6hk",
  authDomain: "react-native-auth-bb111.firebaseapp.com",
  projectId: "react-native-auth-bb111",
  storageBucket: "react-native-auth-bb111.appspot.com",
  messagingSenderId: "52872259218",
  appId: "1:52872259218:web:f57b801b4b16a43c70284b"
};

// Initialize Firebase
let myApp = initializeApp(firebaseConfig);
const auth = getAuth(myApp);



export { auth }