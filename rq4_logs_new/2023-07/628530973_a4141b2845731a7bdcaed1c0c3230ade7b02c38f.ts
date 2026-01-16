import { initializeApp } from 'firebase/app'
import {
  User as FirebaseUser,
  GoogleAuthProvider,
  getAuth,
  signInWithPopup,
  signOut,
} from 'firebase/auth'
import { DocumentData, DocumentSnapshot, doc, getDoc, getFirestore } from 'firebase/firestore'

import { Song } from '../../models/song/Song'
import { SongTag } from '../../models/song/SongTag'
import { MusicianGroup } from '../../models/user/MusicianGroup'
import User, { Permission, UserRole } from '../../models/user/User'
import { usersReference } from '../constants/FirebaseKeys'

const firebaseConfig = {
  apiKey: 'AIzaSyBW830MCh3XcxkCM7qVSi1xukbZHqb9ZA4',
  authDomain: 'music-team-roster.firebaseapp.com',
  projectId: 'music-team-roster',
  storageBucket: 'music-team-roster.appspot.com',
  messagingSenderId: '949364282512',
  appId: '1:949364282512:web:ec4582017533fe1d1d3415',
}

// Set up
export const app = initializeApp(firebaseConfig)
export const googleProvider = new GoogleAuthProvider()
export const auth = getAuth(app)
export const db = getFirestore(app)

// Sign in
export const signInWithGoogle = async () => {
  const result = await signInWithPopup(auth, googleProvider)
  console.log(result)
}
// Sign out
export const logout = () => {
  signOut(auth)
}

// song
interface RawSong {
  id: Song['id']
  name: string
  version: string
  key: Song['key']
  tempo: number
  songUrlString: string
  sheetUrlString: string
  tags: SongTag[]
}

export function songFromSnapshot(snapshot: DocumentSnapshot<DocumentData>): Song {
  const { ...docData } = snapshot.data() as RawSong
  return {
    ...docData,
    id: snapshot.id,
  }
}

// export const auth = firebase.auth()
// const googleProvider = new firebase.auth.GoogleAuthProvider()
// export const signInWithGoogle = () => {
//   auth
//     .signInWithPopup(googleProvider)
//     .then((res) => {
//       console.log(res.user)
//     })
//     .catch((error) => {
//       console.log(error.message)
//     })
// }

// user
interface RawUser {
  email: string
  isLead: boolean
  firstName: string
  lastName: string
  permissions: Permission[]
  countryCode: string
  phoneNumber: string
  availableRoles: UserRole[]
  isInSingapore: boolean
  userId: User['id']
  musicianGroups: MusicianGroup[]
}

export function userFromSnapshot(snapshot: DocumentSnapshot<DocumentData>): User {
  const { ...docData } = snapshot.data() as RawUser
  return {
    ...docData,
    id: snapshot.id,
  }
}

export async function getUserProfile(userId: FirebaseUser['uid']): Promise<User | null> {
  const snapshot = await getDoc(doc(db, usersReference, userId))
  if (!snapshot.exists()) return null
  return userFromSnapshot(snapshot)
}