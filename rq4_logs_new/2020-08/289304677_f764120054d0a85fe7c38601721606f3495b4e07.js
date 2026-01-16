import firebase from 'firebase/app';
import 'firebase/firestore';
import 'firebase/auth';

const config = {
  apiKey: 'AIzaSyAfrxqv8-_c4cJRj2Kd87LtCVdMCQsQnBk',
  authDomain: 'clothing-ecommerce-db-129b5.firebaseapp.com',
  databaseURL: 'https://clothing-ecommerce-db-129b5.firebaseio.com',
  projectId: 'clothing-ecommerce-db-129b5',
  storageBucket: 'clothing-ecommerce-db-129b5.appspot.com',
  messagingSenderId: '863129117234',
  appId: '1:863129117234:web:cef219c116aac98c08922b',
  measurementId: 'G-WCQK5ZK3TL'
};

firebase.initializeApp(config);

export const auth = firebase.auth();
export const firestore = firebase.firestore();

const provider = new firebase.auth.GoogleAuthProvider();
provider.setCustomParameters({ prompt: 'select_account' });
export const signInWithGoogle = () => auth.signInWithPopup(provider);

export default firebase;