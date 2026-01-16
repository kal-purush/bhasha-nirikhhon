import {
  getAuth,
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signOut as logout,
} from 'firebase/auth';

export const signUp = (email: string, password: string): Promise<any> => {
  const auth = getAuth();
  return createUserWithEmailAndPassword(auth, email, password);
};

export const login = (email: string, password: string): Promise<any> => {
  const auth = getAuth();
  return signInWithEmailAndPassword(auth, email, password);
};

export const signOut = (): Promise<void> => {
  const auth = getAuth();
  return logout(auth);
};