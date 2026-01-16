import { doc, getDoc } from "firebase/firestore"
import { db } from "../firebase"

export const getCurrentUserData = async (currentUser) => {
    const userRef = doc(db, 'users', currentUser.uid)
    const userData = (await getDoc(userRef)).data()

    return userData
}