import { auth } from '../firebase/firebaseConfig';
import { createUserWithEmailAndPassword, updateProfile, signInWithEmailAndPassword } from 'firebase/auth';

import Toast from 'react-native-toast-message';

const createUser = async (email: string, password: string, name: string) => {
    try {
        await createUserWithEmailAndPassword(auth, email, password);
        const currentUser = auth.currentUser;
        if (currentUser) {
            await updateProfile(currentUser, { displayName: name });
        }
        return auth.currentUser;
    } catch (error: any) {
        if (error.code === 'auth/email-already-in-use') {
            Toast.show({
                type: 'error',
                text1: 'Error',
                text2: 'Something went wrong'
            });
            return
        }
        if (error.code === 'auth/weak-password') {
            Toast.show({
                type: 'error',
                text1: 'Error',
                text2: 'Password should be at least 6 characters'
            });
            return
        }

        Toast.show({
            type: 'error',
            text1: 'Error',
            text2: error.message
        });
        throw error;
    }
};

const signIn = async (email: string, password: string) => {
    try {
        await signInWithEmailAndPassword(auth, email, password);
        return auth.currentUser;
    } catch (error: any) {
        if (error.code === 'auth/wrong-password') {
            Toast.show({
                type: 'error',
                text1: 'Error',
                text2: 'Wrong password'
            });
            return
        }
        if (error.code === 'auth/user-not-found') {
            Toast.show({
                type: 'error',
                text1: 'Error',
                text2: 'User not found'
            });
            return
        }

        Toast.show({
            type: 'error',
            text1: 'Error',
            text2: error.message
        });
        throw error;
    }
};

const signOut = async () => {
    try {
        await auth.signOut();
    } catch (error: any) {
        Toast.show({
            type: 'error',
            text1: 'Error',
            text2: error.message
        });
        throw error;
    }
};


export { createUser, signIn };