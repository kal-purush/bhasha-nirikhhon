import {
  getAuth,
  createUserWithEmailAndPassword,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  sendPasswordResetEmail,
  signOut,
  User,
  updateProfile,
} from "firebase/auth"
import firebase from "./index"

export const Auth = getAuth(firebase)

export const currentUser = () => {
  return Auth.currentUser
}

export const signUpUser = async (email: string, password: string) => {
  return await createUserWithEmailAndPassword(Auth, email, password)
}

export const listenForAuthState = (callback: (user: User | null) => void) => {
  return onAuthStateChanged(Auth, callback)
}

export const logInUser = async (email: string, password: string) => {
  try {
    return signInWithEmailAndPassword(Auth, email, password).catch((error) => {
      throw new Error(error)
    })
  } catch (error) {
    throw new Error(error as string)
  }
}

export const logoutUser = async () => {
  signOut(Auth)
}

export const passwordReset = async (email: string) => {
  return sendPasswordResetEmail(Auth, email)
    .then(() => {
      return {success: true as const}
    })
    .catch(() => {
      return {error: true as const, message: "Failed to reset password"}
    })
}

export const updateUserName = async (user: User, displayName: string) => {
  return await updateProfile(user, {displayName})
}