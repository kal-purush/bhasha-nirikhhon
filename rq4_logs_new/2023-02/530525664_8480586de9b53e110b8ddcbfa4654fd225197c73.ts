import { getCurrentUser, getUserById} from '../xplat/api';
import { useEffect, useState } from 'react';
import { User } from '../xplat/types';
import { auth } from '../xplat/Firebase';
import { buildUserByIDFetcher, FetchedUserProfile } from './queries';

const useAuth = () => {
  const [authUser, setAuthUser] = useState<FetchedUserProfile>();
  const [loading, setLoading] = useState(true);
  
  useEffect(() =>{
    const unlisten = auth.onAuthStateChanged(
      authUser => {
        setLoading(true);
        console.log(authUser);
        if (authUser) {
          buildUserByIDFetcher(authUser.uid)().then((user) => {
            setAuthUser(user);
            setLoading(false);
          });
        }
      },
    );
    return () => {
      unlisten();
    };
  }, []);

  return { authUser, loading };
};

export default useAuth;