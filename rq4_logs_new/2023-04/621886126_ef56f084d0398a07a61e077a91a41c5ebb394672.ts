import * as React from 'react';
import { User } from 'firebase/auth';
import { auth } from '../firebase/firebaseConfig';

const useAuthentication = () => {
    const [user, setUser] = React.useState<User>();

    React.useEffect(() => {
        const unsubscribeFromAuth = auth.onAuthStateChanged((user) => {
            if (user) {
                setUser(user);
            } else {
                setUser(undefined);
            }
        });

        return unsubscribeFromAuth;
    }, []);

    return { user, setUser };
};

export default useAuthentication;