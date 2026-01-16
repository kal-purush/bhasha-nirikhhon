import { createSlice } from '@reduxjs/toolkit'
import type { PayloadAction } from '@reduxjs/toolkit'
import type { RootState } from '../../app/store'

// Define a type for the slice state
interface ShoppingCartState {
  information: object,
  total: number
}

// Define the initial state using that type
const initialState: ShoppingCartState = {
  information: [],
  total: 0
}
/*
information: [{
    name: '',
    description: '',
    price: 0.00,
    qty: 0
  }],
  total: 0
*/

export const shoppingCartSlice = createSlice({
  name: 'shoppingCart',
  // `createSlice` will infer the state type from the `initialState` argument
  initialState,
  reducers: {
    increment: (state) => {
      state.total += 1
    },
    decrement: (state) => {
      state.total -= 1
    },
    addToCart: (state) => {
      state.total += 1
    },
    removeFromCart: (state) => {
      state.total -= 1
    },
  },
})

export const { increment, decrement, addToCart, removeFromCart } = shoppingCartSlice.actions

// Other code such as selectors can use the imported `RootState` type
export const selectCount = (state: RootState) => state.counter.total

export default shoppingCartSlice.reducer