// @ts-nocheck

import { initializeApp } from 'firebase/app';

import {
  addDoc,
  getFirestore,
  collection,
  getDocs,
} from 'firebase/firestore/lite';

import { collection, getDocs } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: 'AIzaSyCT9VvcPJamAU9j7fVEiI3ulOLzaI985VI',
  authDomain: 'pension-puaka.firebaseapp.com',
  projectId: 'pension-puaka',
  storageBucket: 'pension-puaka.appspot.com',
  messagingSenderId: '427963319177',
  appId: '1:427963319177:web:be2178eaba34b24fec1758',
  measurementId: 'G-H5BQKYNJ46',
};

// Initialize Firebase

const app = initializeApp(firebaseConfig);

const db = getFirestore(app);

export const getDataListFromFirebase = async (collectionName: string) => {
  const querySnapshot = await getDocs(collection(db, collectionName));

  querySnapshot.forEach((doc) => {
    console.log(`${doc.id} => ${doc.data()}`);
  });

  return querySnapshot;
};

export const addDataFromFirebase = async (collectionName: string, data) => {
  try {
    const docRef = await addDoc(collection(db, collectionName), {
      ...data,
    });

    console.log('Document written with ID: ', docRef.id);
  } catch (e) {
    console.error('Error adding document: ', e);
  }
};

export const getDataFromFirebase = async (collectionName) => {
  const db = getFirestore(app);

  const dataCol = collection(db, collectionName);
  const dataSnapshot = await getDocs(dataCol);
  const dataList = dataSnapshot.docs.map((doc) => doc.data());

  return dataList;
};