import {
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail,
} from "firebase/auth";
import { auth } from "@/firebase";

import type { Auth } from "@/types";

export function login(email: string, password: string): Promise<Auth> {
  return signInWithEmailAndPassword(
    auth,
    email,
    password
  ).then((userCredentials) => {
    const { uid, email, providerData } = userCredentials.user;
    return { uid, email, providerData } as Auth;
  });
}

export function register(email: string, password: string): Promise<Auth> {
  return createUserWithEmailAndPassword(
    auth,
    email,
    password
  ).then((userCredentials) => {
    const { uid, email, providerData } = userCredentials.user;
    return { uid, email, providerData };
  });
}

export function forgot(email: string): Promise<void> {
  return sendPasswordResetEmail(auth, email);
}