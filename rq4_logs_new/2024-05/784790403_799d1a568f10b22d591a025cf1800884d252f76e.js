import { GoogleAuthProvider, signInWithPopup } from "firebase/auth"
import { auth } from "../firebase";

function GoogleSignIn() {
    const googleAuthProvider = new GoogleAuthProvider();
    return signInWithPopup(auth, googleAuthProvider)
}

export default GoogleSignIn