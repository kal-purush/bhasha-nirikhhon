import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit'
import { Movie } from '../movies/types'

const nominationsInitialState: Movie[] = []

const nominationsSlice = createSlice({
  name: 'nominations',
  initialState: nominationsInitialState,
  reducers: {
    add: (state, { payload }: PayloadAction<Movie>) => {
      state.push(payload)
    },
    remove: (state, { payload }: PayloadAction<{ imdbID: string }>) => {
      const index = state.findIndex((movie) => movie.imdbID === payload.imdbID)
      if (index !== -1) {
        state.splice(index, 1)
      }
    },
  },
})

export const {
  add: addNominationActionCreator,
  remove: removeNominationActionCreator,
} = nominationsSlice.actions

export default nominationsSlice.reducer