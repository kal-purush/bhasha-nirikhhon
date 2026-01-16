/* eslint-disable no-param-reassign */
import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { Product } from 'src/types/Product';

interface Cart {
  productsList: Product[];
  total: number;
}

const initialState: Cart = {
  productsList: [],
  total: 0,
};

const cartSlice = createSlice({
  name: 'phones',
  initialState,
  reducers: {
    add: (state, action: PayloadAction<Product>) => {
      state.productsList.push(action.payload);
      state.total += action.payload.price;
    },
    remove: (state, action: PayloadAction<number>) => {
      const indexToRemove = state.productsList.findIndex(
        (product) => product.id === action.payload,
      );

      if (indexToRemove !== -1) {
        state.total -= state.productsList[indexToRemove].price;
        state.productsList.splice(indexToRemove, 1);
      }
    },
  },
});

export default cartSlice.reducer;
export const { actions } = cartSlice;