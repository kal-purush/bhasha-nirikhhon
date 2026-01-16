import Firebase from 'firebase';

const firebaseApp = Firebase.initializeApp({
  	
});

export const db = firebaseApp.database();
export const auth = firebaseApp.auth();