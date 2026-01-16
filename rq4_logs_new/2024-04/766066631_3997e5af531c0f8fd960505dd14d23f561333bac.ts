import { createAsyncThunk, createSlice } from '@reduxjs/toolkit'
import { format } from 'date-fns'
import {
  collection,
  doc,
  getDoc,
  getDocs,
  setDoc,
  Timestamp,
  updateDoc,
} from 'firebase/firestore'
import { watch } from 'fs'

import { db } from '@/lib/firebase'
import { MovieItem } from '@/types/Lists'

type ListType = 'favorites' | 'watchlist' | 'custom'

interface moviesState {
  movieListData: Record<string, MovieItem> // 各ユーザーのリストをuidをキーとして保持する
  status: 'idle' | 'loading' | 'succeeded' | 'failed'
  error: string | undefined
}

const initialState: moviesState = {
  movieListData: {},
  status: 'idle',
  error: undefined,
}

// 新規データベース構造 start
export const fetchUserLists = createAsyncThunk<
  { uid: string; movieListData: Record<string, MovieItem> },
  string,
  { rejectValue: { message: string; error?: any } }
>('movies/fetchUserLists', async (uid, { rejectWithValue }) => {
  try {
    const userListRef = doc(db, 'users', uid)
    const querySnapshot = await getDocs(collection(userListRef, 'movies'))

    const movieListData: Record<string, MovieItem> = {}

    querySnapshot.forEach((doc) => {
      const data = doc.data() as MovieItem

      if (data.watchedAt) {
        const watchedAt = data.watchedAt.toDate()

        const formattedWatchedAt = watchedAt
          ? format(watchedAt, 'yyyy-MM-dd')
          : null

        movieListData[doc.id] = {
          ...data,
          watchedAt: formattedWatchedAt,
        }
      } else {
        movieListData[doc.id] = data
      }
    })

    return { uid, movieListData }
  } catch (error: any) {
    return rejectWithValue({ message: 'Failed to fetch user lists', error })
  }
})
// 新規データベース構造 end

const moviesSlice = createSlice({
  name: 'movies',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(fetchUserLists.pending, (state) => {
        state.status = 'loading'
      })
      .addCase(fetchUserLists.fulfilled, (state, action) => {
        state.status = 'succeeded'
        const { uid, movieListData } = action.payload
        state.movieListData[uid] = movieListData
      })
      .addCase(fetchUserLists.rejected, (state, action) => {
        state.status = 'failed'
        if (action.payload) {
          state.error = action.payload.message
          console.error('Fetch user lists error:', action.payload.error)
        } else {
          state.error = action.error.message
        }
      })
  },
})

export default moviesSlice.reducer