import { db } from '@/lib/firebase/clientApp';
import { doc, setDoc } from 'firebase/firestore';

// export async function getRestaurantById(db, restaurantId) {
//   if (!restaurantId) {
//     console.log('Error: Invalid ID received: ', restaurantId);
//     return;
//   }
//   const docRef = doc(db, 'restaurants', restaurantId);
//   const docSnap = await getDoc(docRef);
//   return {
//     ...docSnap.data(),
//     timestamp: docSnap.data().timestamp.toDate(),
//   };
// }

// working on adding users if they dont exist via firestore.rules and this
export async function addUserToDb(userId: string) {
  const docRef = doc(db, 'users', userId);
  return await setDoc(docRef, { userId });
}