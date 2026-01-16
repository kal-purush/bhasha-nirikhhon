import { createSlice } from '@reduxjs/toolkit'

export const appSlice = createSlice({
  initialState: {
    isLoggedIn: false,
  },
  name: 'app',
  reducers: create => ({
    setIsLoggedIn: create.reducer<{ isLoggedIn: boolean }>((state, action) => {
      state.isLoggedIn = action.payload.isLoggedIn
    }),
  }),
  selectors: {
    selectIsLoggedIn: state => state.isLoggedIn,
  },
})

export const { setIsLoggedIn } = appSlice.actions
export const { selectIsLoggedIn } = appSlice.selectors
export const appReducer = appSlice.reducer