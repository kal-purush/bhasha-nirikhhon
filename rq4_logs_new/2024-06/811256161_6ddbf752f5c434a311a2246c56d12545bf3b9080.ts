import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { RootState } from '@store';
import { clearCartCache, getCartCache, setCartCache } from '@utils/localStorageActions';
import { CartCache, CartItem, CartItemQuantityAction } from 'types/cart';

export interface CartState {
  cache: CartCache;
}

const initialState: CartState = {
  cache: getCartCache(),
};

const cartSlice = createSlice({
  name: 'cart',
  initialState,
  reducers: {
    addCartItem: (state, action: PayloadAction<CartItem>) => {
      const { quantity, product } = action.payload;
      const existingItem = state.cache.items.find((item) => item.product.id == product.id);

      if (existingItem) {
        existingItem.quantity += quantity;
        existingItem.total = Number.parseFloat(
          Number(existingItem.total + existingItem.product.price * quantity).toFixed(2)
        );
      } else {
        state.cache.items.push(action.payload);
      }

      state.cache.totalPrice = Number.parseFloat(
        Number(state.cache.totalPrice + product.price * quantity).toFixed(2)
      );

      setCartCache(state.cache);
    },
    removeCartItem: (state, action: PayloadAction<number>) => {
      const deletedItem = state.cache.items.splice(action.payload, 1);
      state.cache.totalPrice -= deletedItem[0].total;

      setCartCache(state.cache);
    },
    setCartItemQuantity: (state, action: PayloadAction<CartItemQuantityAction>) => {
      const { index, type } = action.payload;
      const cartItem = state.cache.items.at(index);

      if (cartItem) {
        if (type == 'increment') {
          cartItem.quantity++;
          cartItem.total = Number.parseFloat(
            Number(cartItem.total + cartItem.product.price).toFixed(2)
          );
          state.cache.totalPrice = Number.parseFloat(
            Number(state.cache.totalPrice + cartItem.product.price).toFixed(2)
          );
        }
        if (type == 'decrement') {
          cartItem.quantity--;
          cartItem.total = Number.parseFloat(
            Number(cartItem.total - cartItem.product.price).toFixed(2)
          );
          state.cache.totalPrice = Number.parseFloat(
            Number(state.cache.totalPrice - cartItem.product.price).toFixed(2)
          );
        }
      }

      setCartCache(state.cache);
    },
    clearCart: (state) => {
      state.cache.items.length = 0;
      state.cache.totalPrice = 0;

      clearCartCache();
    },
  },
});

export const { addCartItem, setCartItemQuantity, removeCartItem, clearCart } = cartSlice.actions;

export const selectCartCache = (state: RootState) => state.cart.cache;

export default cartSlice.reducer;