import { ActionCodeSettings, browserSessionPersistence, getAdditionalUserInfo, getAuth, GoogleAuthProvider, sendSignInLinkToEmail, setPersistence, signInWithPopup, UserCredential } from "firebase/auth";
import { initializeFirebaseApp } from "./firebaseApp";

const app = initializeFirebaseApp();
export const auth = getAuth( app );

export async function signInWithGoogle() 
{
  const provider = new GoogleAuthProvider();

  await setPersistence( auth, browserSessionPersistence );

  const userCredential = await signInWithPopup( auth, provider );

  return userCredential;
}

export function additionalUserInfo( userCredential: UserCredential )
{
  let userInfo = getAdditionalUserInfo( userCredential );

  return userInfo;
}

const actionCodeSettings: ActionCodeSettings = {
  // URL to redirect back to
  url: "localhost:3000", // ? window.location.href
  handleCodeInApp: true,
};

export async function signInWithEmail( email: string )
{
  try { 
    await sendSignInLinkToEmail( auth, email, actionCodeSettings );
    window.localStorage.setItem( "emailForSignIn", email );
  }
  catch( error: any ) {
    console.log( error.code, error.message );
  }
}