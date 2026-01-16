import { createSlice, PayloadAction } from '@reduxjs/toolkit'

interface ContributionsState {
  contributions: any[]
}

const init: ContributionsState = {
  contributions: [],

}

export const contributionsSlice = createSlice({
  name: 'contributions',
  initialState: init,
  reducers: {
    storeContributions(state, action: PayloadAction<any[]>) {
      state.contributions = action.payload
    }
    
  },
})

export const contributionsActions = contributionsSlice.actions