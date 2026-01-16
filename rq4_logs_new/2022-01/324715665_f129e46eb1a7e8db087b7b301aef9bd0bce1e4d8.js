import firebase from './config';
const auth = firebase.auth;
const adminAuth = firebase.adminAuth
const db = firebase.storage;

export const registerWithEmailAndPassword = async (email, password) => {
    try {
        firebase.getAlluser();
    } catch (err) {
        console.error(err);
        alert(err.message);
    }
  };