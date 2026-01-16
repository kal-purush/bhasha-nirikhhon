import { initializeApp } from "firebase/app";
import { getAuth, GoogleAuthProvider, signInWithPopup } from "firebase/auth";
import { getDocs, addDoc, query, orderBy, limit, serverTimestamp  } from "firebase/firestore";
import { useCollectionData } from "react-firebase-hooks/firestore";
import { collection } from "firebase/firestore";
import { getFirestore } from "firebase/firestore";
import { RefObject } from "react";


export const signIn = () => {
    const auth = getUserAuth(false);
    const provider = new GoogleAuthProvider();
    signInWithPopup(auth, provider);
}

export const signOut = () => {
    const auth = getUserAuth(false);
    auth.signOut();
}   

// Do we need to call initializeFirebase() in the code first
// Then call getUserAuth, or does this work?
export const getUserAuth = (alreadyInit: boolean) => {
    if (!alreadyInit) {
        const app = initializeFirebase();
    }
    const auth = getAuth();
    return auth;
}

export const getFireStore = (alreadyInit: boolean) => {
    if (!alreadyInit) {
        const app = initializeFirebase();
    }
    const firestore = getFirestore();
    return firestore;
}

export const initializeFirebase = () => {
    const firebaseConfig = {
        apiKey: "AIzaSyCVZcN1p21TgNA-jSx9x7qhCAmwf8Sei8E",
        authDomain: "me2-web-app.firebaseapp.com",
        projectId: "me2-web-app",
        storageBucket: "me2-web-app.appspot.com",
        messagingSenderId: "100835264460",
        appId: "1:100835264460:web:6af0681fa12e244fca3f69",
        measurementId: "G-L2NXZ8F0B2",
      };

      const app = initializeApp(firebaseConfig);
      return app;
}

export const getMessages = () => {
    const firestore = getFireStore(false);

    const messagesRef = collection(firestore, "messages");
    const messagesQuery = query(messagesRef, orderBy("createdAt"), limit(25));
    const [messages] = useCollectionData(messagesQuery);
    return messages;
}  

export const sendMessage = async (e: React.FormEvent<HTMLFormElement>, formValue : string, setFormValue : Function, dummy: RefObject<any>) => {
    const app = initializeFirebase();
    const auth = getUserAuth(true);
    const firestore = getFireStore(true);

    e.preventDefault();

    if (auth.currentUser) {
      const { uid } = auth.currentUser;

      try {
        await addDoc(collection(firestore, "messages"), {
          text: formValue,
          createdAt: serverTimestamp(),
          uid
        });
        setFormValue('');
        if (dummy.current) {
          dummy.current.scrollIntoView({ behavior: "smooth" });
        }
      } catch (error) {
        console.error("Error adding document: ", error);
      }
    }
  };