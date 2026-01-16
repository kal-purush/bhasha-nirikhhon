import { SIGN_IN, SIGN_OUT } from "./types";
import firebaseApp from '../firebase';

export const signIn = (userId) => {
  return {
    type: SIGN_IN,
    payload: userId
  }
}

export const signOut = () => {
  firebaseApp.auth().signOut().then(() => {
    console.log('User signed out')
  });
  return {
    type: SIGN_OUT
  }
}