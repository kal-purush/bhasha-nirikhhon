import { initializeApp } from 'firebase/app';
import { collection, getDocs, getFirestore } from 'firebase/firestore';
import { getAuth } from 'firebase/auth';

const firebaseConfig = {
    apiKey: 'AIzaSyAmZT82QV9MMdGLBbtm9phSZbi1T97k1Tg',
    authDomain: 'fe---solidjs-glider-509bf.firebaseapp.com',
    projectId: 'fe---solidjs-glider-509bf',
    storageBucket: 'fe---solidjs-glider-509bf.appspot.com',
    messagingSenderId: '432988647524',
    appId: '1:432988647524:web:ecc9ca65d470fa32866f71',
    measurementId: 'G-FXJJ0WTCNX'
};

export const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
export const firebaseAuth = getAuth(app);

export const getUsers = async () => {
    const usersCol = collection(db, 'users');
    const usersSnap = await getDocs(usersCol);
    const userList = usersSnap.docs.map((doc) => doc.data());
    return userList;
};