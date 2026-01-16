import { useState, useEffect } from 'react';
import { onAuthStateChanged } from "firebase/auth";
import { auth } from '../lib/firebase';
import { createUserProfile } from '../lib/user';

export default function useAuth() {
  const [user, setUser] = useState(null);

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, (authUser) => {
      setUser(authUser);
      if (authUser) {
        // Create or update user profile in Firestore
        createUserProfile(authUser).catch(console.error);
      }
    });

    return () => unsubscribe();
  }, []);

  return user;
}