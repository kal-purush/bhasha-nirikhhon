import { initializeApp } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
    apiKey: "AIzaSyB2Q0bxwqDS8gqeqsv7B8cEC2ZGbfH_Cv4",
    authDomain: "language-learning-app-ea393.firebaseapp.com",
    projectId: "language-learning-app-ea393",
    storageBucket: "language-learning-app-ea393.appspot.com",
    messagingSenderId: "1000279609278",
    appId: "1:1000279609278:web:716fabd1cdd808c71473ef"
};

const app = initializeApp(firebaseConfig);

const firestore = getFirestore(app);

export { firestore };