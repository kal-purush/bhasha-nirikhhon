import { Goal } from 'types/goal'
import { createEntityAdapter, createSlice } from '@reduxjs/toolkit'
import { fetchGoals } from './actions'

export const goalsAdapter = createEntityAdapter({
  selectId: (goal: Goal) => goal.id
})

export const goalsSlice = createSlice({
  name: 'goals',
  initialState: goalsAdapter.getInitialState({
    loading: false,
    error: false
  }),
  reducers: {},
  extraReducers: builder => {
    // fetchGoals
    builder.addCase(fetchGoals.pending, state => {
      state.loading = true
    })
    builder.addCase(fetchGoals.fulfilled, (state, action) => {
      state.loading = false
      goalsAdapter.setAll(state, action.payload)
    })
    builder.addCase(fetchGoals.rejected, state => {
      state.loading = false
      state.error = true
    })
  }
})

export default goalsSlice.reducer