// ** Redux Imports
import { createSlice } from '@reduxjs/toolkit'

// ** actions
import { serviceName } from './actions'

const initialState = {
  orderItems: []
}

export const orderProductSlice = createSlice({
  name: serviceName,
  initialState,
  reducers: {
    addProductToCart: (state, action) => {
      state.orderItems = action?.payload?.orderItems
    }
  },
  extraReducers: builder => {}
})

export default orderProductSlice.reducer
export const { addProductToCart } = orderProductSlice.actions