import {
  signInWithCredential,
  GoogleAuthProvider,
  signInWithPopup,
} from "firebase/auth";
import { auth } from "./init";
import { isExtension } from "../utils/common";

export const signIn = async () => {
  return isExtension() ? extensionSignin() : browserSignin();
};

const extensionSignin = async () => {
  const authTokenResult = await chrome.identity.getAuthToken({
    interactive: true,
  });

  const userCredential = await signInWithCredential(
    auth,
    GoogleAuthProvider.credential(null, authTokenResult.token),
  );

  return userCredential;
};

const browserSignin = async () => {
  const provider = new GoogleAuthProvider();
  provider.addScope("email");
  provider.setCustomParameters({
    prompt: "select_account",
  });

  const userCredential = await signInWithPopup(auth, provider);

  return userCredential;
};

export const signOut = async () => {
  return auth.signOut();
};