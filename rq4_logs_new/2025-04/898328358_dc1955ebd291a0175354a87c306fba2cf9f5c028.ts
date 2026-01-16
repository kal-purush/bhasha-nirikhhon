import { doc, getDoc, setDoc, deleteDoc } from "firebase/firestore";
import { db } from "@/firebase";

// お気に入りを取得
export const getFavoriteSchools = async (uid: string) => {
  const userRef = doc(db, "users", uid);
  const favoritesRef = doc(userRef, "favorites", "list");
  const snap = await getDoc(favoritesRef);
  return snap.exists() ? snap.data() : {};
};

// お気に入りを追加・削除
export const toggleFavoriteSchool = async (
  uid: string,
  schoolId: string,
  isLiked: boolean
) => {
  const docRef = doc(db, "users", uid, "favorites", schoolId);
  if (isLiked) {
    await setDoc(docRef, { liked: true });
  } else {
    await deleteDoc(docRef);
  }
};