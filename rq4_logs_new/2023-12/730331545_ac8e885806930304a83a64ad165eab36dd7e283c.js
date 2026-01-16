import { initializeApp } from 'firebase/app';
import { getAuth, GoogleAuthProvider } from 'firebase/auth';

const firebaseConfig = {
  apiKey: 'AIzaSyDTDvY07mpdiIvaW9yxVA1BIMno-qbcoPc',
  authDomain: 'video-32ddf.firebaseapp.com',
  projectId: 'video-32ddf',
  storageBucket: 'video-32ddf.appspot.com',
  messagingSenderId: '241270813842',
  appId: '1:241270813842:web:8e2276217932dc99e6e24b',
};

const app = initializeApp(firebaseConfig);
export const auth = getAuth();
export const provider = new GoogleAuthProvider();

export default app;