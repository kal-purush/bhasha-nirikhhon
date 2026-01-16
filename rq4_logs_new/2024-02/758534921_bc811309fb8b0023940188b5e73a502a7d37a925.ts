import { createAsyncThunk, createSlice, PayloadAction } from '@reduxjs/toolkit'
import { ICategory } from '../../common/interfaces'
import { getCategories } from '../../api'

const initialState: {
  list: ICategory[]
  status: string
} = {
  list: [],
  status: 'loading',
}

export const categoriesSlice = createSlice({
  name: 'categories',
  initialState,
  reducers: {
    setCategories: (state, action: PayloadAction<ICategory[]>) => {
      console.log(state, action)
      state.list = action.payload
    },
  },
  extraReducers(builder) {
    builder
      .addCase(
        fetchCategories.pending,
        (
          state: {
            list: ICategory[]
            status: string
          },
        ) => {
          state.status = 'loading'
        },
      )
      .addCase(
        fetchCategories.fulfilled,
        (
          state: {
            list: ICategory[]
            status: string
          },
          action: any,
        ) => {
          state.list = action.payload
          state.status = 'loaded'
        },
      )
  },
})

export const fetchCategories = createAsyncThunk('categories/fetchCategories', async () => {
  try {
    return await getCategories()
  } catch (error: any) {
    return error.message
  }
})

export const { setCategories } = categoriesSlice.actions

export default categoriesSlice.reducer