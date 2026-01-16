import { User } from 'types/profile'
import { createEntityAdapter, createSlice } from '@reduxjs/toolkit'
import { fetchUser } from './actions'

export const userAdapter = createEntityAdapter({
  selectId: (user: User) => user.id
})

export const usersSlice = createSlice({
  name: 'user',
  initialState: userAdapter.getInitialState({
    loading: false,
    error: false
  }),
  reducers: {
    resetUserError: state => {
      state.error = false
    }
  },
  extraReducers: builder => {
    builder.addCase(fetchUser.pending, state => {
      state.loading = true
    })
    builder.addCase(fetchUser.fulfilled, (state, action) => {
      state.loading = false
      userAdapter.setAll(state, action.payload)
    })
    builder.addCase(fetchUser.rejected, state => {
      state.loading = false
      state.error = true
    })
  }
})

export const { resetUserError } = usersSlice.actions
export default usersSlice.reducer