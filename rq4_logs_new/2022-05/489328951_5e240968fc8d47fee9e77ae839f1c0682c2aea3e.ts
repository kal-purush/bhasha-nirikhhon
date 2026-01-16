import Router from 'next/router';
import { GoogleAuthProvider, signInWithPopup, signOut } from 'firebase/auth';
import { auth } from '../firebase/index';
import { User } from '../types/user';

export const listenAuthState = (): Promise<User | undefined> => {
  return new Promise((resolve, reject) => {
    auth.onAuthStateChanged((user) => {
      if (!user) {
        Router.push('/login');
        reject(undefined);
      } else {
        Router.push('/');
        resolve({
          uid: user.uid,
        });
      }
    });
  });
};

export const login = async () => {
  const provider = new GoogleAuthProvider();
  await signInWithPopup(auth, provider)
    .then(() => {})
    .catch((e) => {
      alert(e.message);
    });
};

export const logout = async () => {
  await signOut(auth).catch((e) => {
    alert(e.message);
  });
};