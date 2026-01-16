import { getAuth } from 'firebase/auth'
import { doc, getDoc } from 'firebase/firestore'

import { db } from '@/lib/firebase'

export const getUid = async (): Promise<string | null> => {
  const auth = getAuth()
  const user = auth.currentUser
  if (user) {
    return user.uid
  } else {
    return null
  }
}

export const checkIsFavorite = async (movieId: string, uid: string) => {
  try {
    const userListRef = doc(db, 'users', uid)
    const favoritesRef = doc(userListRef, 'favorites', movieId)
    const favoritesDoc = await getDoc(favoritesRef)

    return await favoritesDoc.exists()
  } catch (error) {
    throw new Error('Failed to fetch search results')
  }
}