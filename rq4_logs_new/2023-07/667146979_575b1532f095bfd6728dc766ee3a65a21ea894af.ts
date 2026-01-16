import { getAuth, signInWithEmailAndPassword, signOut } from "firebase/auth";
import { app } from "@/src/firebase/Firebase";

export const auth = getAuth(app);

export const authenticate = (email : string, password: string) => {
    signInWithEmailAndPassword(auth, email, password)
        .then((response)=>{
            window.location.href = "/";
        })
        .catch((error)=>{
            alert(error);
            window.location.href='/login';
        })
    
}

export const logout = () => {
    signOut(auth)
    .then(()=>window.location.href = "/")
    .catch((err)=>console.log("error"));
}