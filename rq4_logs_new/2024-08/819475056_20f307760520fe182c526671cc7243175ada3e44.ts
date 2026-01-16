import {createSlice, PayloadAction} from "@reduxjs/toolkit"
import {Category} from "@/common/types"

const initialState: GirlsState = {
  selected_category: 'ALL'
}

const girlsSlice = createSlice ({
  initialState,
  name: 'girls',
  reducers: {
    setCategory(state, action: PayloadAction<GirlsCategory>) {
      state.selected_category = action.payload
    },
  },
  selectors: {
    GirlsSelected_categorySelector: state => state.selected_category
  }
})

export const girlsActions = girlsSlice.actions
export const girlsReducer = girlsSlice.reducer
export const { GirlsSelected_categorySelector } = girlsSlice.selectors

type GirlsState = {
  selected_category: GirlsCategory
}

type GirlsCategory = 'ALL' | '2010 - 2011' | '2012 - 2013'