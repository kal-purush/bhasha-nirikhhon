import { useState, useEffect } from 'react';
import { getUserByUserId } from '../services/firebase';

export default function useUser(uid: string) {
  const [activeUser, setActiveUser] = useState<any>();

  useEffect(() => {
    async function getUserObjByUserId(uid: string) {
      const user = await getUserByUserId(uid);
      setActiveUser(user);
    }

    if (uid) {
      getUserObjByUserId(uid);
    }
  }, [uid]);

  return { user: activeUser, setActiveUser };
}