import { createSlice } from '@reduxjs/toolkit'

const initialState = {
  currentPage: 1
}

export const characterSlice = createSlice({
  name: 'character',
  initialState,
  reducers: {
    setCurrentPage: (state, { payload }) => {
      state.currentPage = payload
    }
  }
})

export const { setCurrentPage } = characterSlice.actions
export default characterSlice.reducer