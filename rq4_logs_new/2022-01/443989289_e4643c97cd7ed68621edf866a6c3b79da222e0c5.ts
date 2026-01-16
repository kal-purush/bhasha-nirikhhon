import { doc, getDoc } from 'firebase/firestore';
import { getFirestore } from 'firebase/firestore';

// TODO: Sumeet create a User interface
export const getUserDetails = async (uid: string): Promise<any> => {
  const db = getFirestore();
  const docRef = doc(db, 'users', uid);
  const docSnap = await getDoc(docRef);
  const exists = await docSnap.exists();
  const data = docSnap.data();
  return { exists, data };
};