import { createSlice } from '@reduxjs/toolkit'

const slice = createSlice({
  name: 'order',
  initialState: {
    currentOrders: [],
    historyOrders: [],
    requestFetch: true
  },
  reducers: {
    setCurrentOrders: (state, action) => {
      state.currentOrders = action.payload
    },
    setHistoryOrders: (state, action) => {
      state.historyOrders = action.payload
    },
    setRequestFetch: (state, action) => {
      state.requestFetch = action.payload
    },
    moveCurrentToHistory: (state, action) => {
      const order = state.currentOrders.find(
        (order) => order.orderid === action.payload
      )
      const temp = state.currentOrders.splice(action.payload)
      state.historyOrders.push(order)
      state.currentOrders = temp
    }
  }
})

export const orderActions = slice.actions
export const orderReducer = slice.reducer