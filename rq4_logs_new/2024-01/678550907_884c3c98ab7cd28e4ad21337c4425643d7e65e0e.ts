// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { 
    signInWithPopup,
    signInWithRedirect,
    getAuth,
    GoogleAuthProvider,
    createUserWithEmailAndPassword,
    signInWithEmailAndPassword,
    signOut,
    onAuthStateChanged,
    User,
    NextOrObserver
} from "firebase/auth";

import { 
    getFirestore, 
    collection, 
    doc, 
    getDocs, 
    getDoc,
    query,
    where,
    setDoc,
} from "firebase/firestore";

// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
    apiKey: "AIzaSyCxGAYfm28IJUj01Djbz6WuBsCqJClkKH4",
    authDomain: "vanlife-db.firebaseapp.com",
    projectId: "vanlife-db",
    storageBucket: "vanlife-db.appspot.com",
    messagingSenderId: "464785505481",
    appId: "1:464785505481:web:69b2025b743c2151e62118"
};

// Initialize Firebase
const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp)

const googleProvider = new GoogleAuthProvider();
googleProvider.setCustomParameters({
    prompt: "select_account"
});

export const auth = getAuth()
export const signInWithGooglePopup = () => signInWithPopup(auth, googleProvider);
export const signInWithGoogleRedirect = () => signInWithRedirect(auth, googleProvider);

// Refactoring the fetching functions
const vansCollectionRef = collection(db, "vans")

export type AdditionalInformation = {
    displayName?: string;
}

export type Van = {
    id: string;
    name: string;
    price: number;
    type: string;
    hostId: string;
    description: string;
    imageUrl: string;
}

export const createUserDocumentFromAuth = async (userAuth: User, additionalInformation?: AdditionalInformation) => {
    if(!userAuth) return

    const userDocRef = doc(db, 'users', userAuth.uid)
    const userSnapshot = await getDoc(userDocRef);

    if(!userSnapshot.exists()) {
        const { displayName, email } = userAuth;
        const createdAt = new Date();
        try{
            await setDoc(userDocRef, {
                displayName,
                email,
                createdAt,
                ...additionalInformation,
            })
        } catch(error) {
            console.error('Error creating user: ',error);
        }
    }

    return userDocRef
}

export const createAuthUserWithEmailAndPassword = async ( email: string, password: string ) => {
    if(!email || !password) return
    
    return await createUserWithEmailAndPassword(auth, email, password);
}

export const signInAuthUserWithEmailAndPassword = async ( email: string, password: string ) => {
    if(!email || !password) return
    
    return await signInWithEmailAndPassword(auth, email, password);
}

export const signOutUser = async () => await signOut(auth)

export const onAuthStateChangedListener = (callback: NextOrObserver<User>) => {
    if(!callback) return
    
    return onAuthStateChanged(auth, callback)
}

export const getVans = async () => {
    const querySnapshot = await getDocs(vansCollectionRef)  
    const dataArr = querySnapshot.docs.map(doc => ({
        ...doc.data(),
        id: doc.id
    }))
    
    return dataArr as Van[]
}

export const getVan = async (id: string) => {
    const docRef = doc(db, "vans", id)
    const vanSnapshot = await getDoc(docRef)
    
    return {
        ...vanSnapshot.data(),
        id: vanSnapshot.id
    } as Van
}

export const getHostVans = async() => {
    const q = query(vansCollectionRef, where("hostId", "==", "123"))
    const querySnapshot = await getDocs(q)
    const dataArr = querySnapshot.docs.map(doc => ({
        ...doc.data(),
        id: doc.id
    }))
    
    return dataArr
}

// export async function loginUser(creds) {
//     const res = await fetch("/api/login",
//         { method: "post", body: JSON.stringify(creds) }
//     )
//     const data = await res.json()

//     if(!res.ok) {
//         throw {
//             message: data.message,
//             statusText: res.statusText,
//             status: res.status
//         }
//     }
//     return null
// }