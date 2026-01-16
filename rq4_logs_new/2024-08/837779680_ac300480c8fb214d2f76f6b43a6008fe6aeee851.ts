// Import the functions you need from the SDKs you need
import { initializeApp } from 'firebase/app';
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: 'AIzaSyA_e9ef1gzve2eHlpMCY3q0WrCnMX0fMaY',
  authDomain: 'tap-e-commerce-cdn.firebaseapp.com',
  projectId: 'tap-e-commerce-cdn',
  storageBucket: 'tap-e-commerce-cdn.appspot.com',
  messagingSenderId: '513328521158',
  appId: '1:513328521158:web:c9520ce0607e8671275cc7',
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

export default app;