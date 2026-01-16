import { db } from "@/firebase";
import {
  collection,
  addDoc,
  doc,
  getDocs,
  query,
  where,
  setDoc,
} from "firebase/firestore";
export async function addUser(email: string, name: string) {
  try {
    const docRef = await addDoc(collection(db, "users"), {
      email,
      name,
    });
    console.log("Document written with ID: ", docRef.id);
  } catch (e) {
    console.error("Error adding document: ", e);
  }
}
export async function userExist(email: string) {
  try {
    const usersCollectionRef = collection(db, "users");
    const q = query(usersCollectionRef, where("email", "==", email));
    const querySnapshot = await getDocs(q);
    return !querySnapshot.empty;
  } catch (e) {
    console.error("Error checking user document: ", e);
  }
}
export async function addFavoriteMovie(email: string, movieId: number) {
  try {
    const usersCollectionRef = collection(db, "users");

    const q = query(usersCollectionRef, where("email", "==", email));
    const querySnapshot = await getDocs(q);

    if (querySnapshot.empty) {
      console.log("No user found with the provided email.");
      return;
    }

    const userDocRef = querySnapshot.docs[0].ref;

    const favoriteMoviesCollectionRef = collection(
      userDocRef,
      "favoriteMovies"
    );

    const movieDocRef = doc(favoriteMoviesCollectionRef, `${movieId}`);
    await setDoc(movieDocRef, { movieId });
  } catch (e) {
    console.error("Error adding favorite movie: ", e);
  }
}