import { initializeApp } from 'firebase/app';
import { getAuth } from "firebase/auth";

// Optionally import the services that you want to use
// import {...} from "firebase/database";
// import {...} from "firebase/firestore";
// import {...} from "firebase/functions";
// import {...} from "firebase/storage";

// Initialize Firebase
const firebaseConfig = {
    apiKey: "AIzaSyCsSmDc2bfG3b-X0NH2GbKyrO3lUz-jXmU",
    authDomain: "alternova-c75ad.firebaseapp.com",
    projectId: "alternova-c75ad",
    storageBucket: "alternova-c75ad.appspot.com",
    messagingSenderId: "98816705456",
    appId: "1:98816705456:web:5ce1c5ebee205bf7c74c75"
};

const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);

export default app;
// For more information on how to access Firebase in your project,
// see the Firebase documentation: https://firebase.google.com/docs/web/setup#access-firebase