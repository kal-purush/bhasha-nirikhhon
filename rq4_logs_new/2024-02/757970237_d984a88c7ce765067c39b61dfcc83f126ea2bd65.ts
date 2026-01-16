import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { Product } from 'src/types/Product';
import { getItemFromStorage, setItemToStorage } from '../storage';

interface Favorites {
  favoritesList: Product[];
}

const initialState: Favorites = {
  favoritesList: getItemFromStorage('favorites', []),
};

const cartSlice = createSlice({
  name: 'favorites',
  initialState,
  reducers: {
    add: (state, action: PayloadAction<Product>) => {
      state.favoritesList.push(action.payload);
      setItemToStorage('favorites', state.favoritesList);
    },
    remove: (state, action: PayloadAction<number>) => {
      const indexToRemove = state.favoritesList.findIndex(
        (product) => product.id === action.payload,
      );

      if (indexToRemove !== -1) {
        state.favoritesList.splice(indexToRemove, 1);
        setItemToStorage('favorites', state.favoritesList);
      }
    },
  },
});

export default cartSlice.reducer;
export const { actions } = cartSlice;