import {
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
} from "firebase/auth";
import { doc, setDoc } from "firebase/firestore";
import { db, auth } from "../utils/firebaseConfig";
import { User } from "@/models/User";

export const signup = async (
  email: string,
  password: string,
  firstname: string,
  lastname: string
) => {
  try {
    const userCredential = await createUserWithEmailAndPassword(
      auth,
      email,
      password
    );
    const user = userCredential.user;
    if (user) {
      await setDoc(doc(db, "users", user.uid), {
        firstName: firstname,
        lastName: lastname,
        email: email,
        balance: 0,
        createdAt: new Date(),
      });
      console.log("User created and added to Firestore:", user.uid);
    }
  } catch (error) {
    if (error instanceof Error) {
      throw new Error(error.message);
    } else {
      throw new Error("An error occurred during signup.");
    }
  }
};

// signIn function
export const signIn = async (
  email: string,
  password: string
): Promise<void> => {
  try {
    const userCredential = await signInWithEmailAndPassword(
      auth,
      email,
      password
    );
    console.log(
      "User signed in successfully with email:",
      userCredential.user.email
    );
  } catch (error) {
    if (error instanceof Error) {
      console.error("Sign-in error:", error.message);
      throw error;
    }
  }
};