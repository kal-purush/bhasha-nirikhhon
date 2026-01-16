import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { priceWithTaxToDollars } from '../../utilities/priceWithTaxToDollars';
import { ISneaker } from '../sneakers/sneakers.model';
import { ICart, ICartItem } from './cart.model';

const initialState: ICart = {
   cartItems: [],
   totalItems: 0,
};

const cartSlice = createSlice({
   name: 'cart',
   initialState,
   reducers: {
      updateCartItem: (
         state,
         action: PayloadAction<{ id: string; type: 'increase' | 'descrease' }>
      ) => {
         const countChangeNumber = action.payload.type === 'increase' ? 1 : -1;

         state.cartItems = state.cartItems.map((item: ICartItem) => {
            if (item.sneaker.productId === action.payload.id) {
               return {
                  ...item,
                  count: item.count + countChangeNumber,
               };
            }

            return item;
         });

         state.totalItems += countChangeNumber;
      },
      addCartItem: (state, action: PayloadAction<ISneaker>) => {
         const item = state.cartItems.find(
            (item) => item.sneaker.productId === action.payload.productId
         );

         if (item) {
            state.cartItems = state.cartItems.map((item: ICartItem) => {
               if (item.sneaker.productId === action.payload.productId) {
                  return {
                     ...item,
                     count: item.count + 1,
                  };
               }

               return item;
            });
         } else {
            state.cartItems.push({
               sneaker: action.payload,
               count: 1,
               price: priceWithTaxToDollars(action.payload.priceWithTax.min),
            });
         }

         state.totalItems += 1;
      },
      removeCartItem: (state, action: PayloadAction<{ id: string }>) => {
         state.cartItems = state.cartItems.filter((item: ICartItem) => {
            if (item.sneaker.productId === action.payload.id) {
               state.totalItems -= item.count;
            }

            return item.sneaker.productId !== action.payload.id;
         });
      },
   },
});

export const getCartItemsSelector = (store: { cart: ICart }): ICartItem[] =>
   store.cart.cartItems;
export const getCartTotalItemsSelector = (store: { cart: ICart }) =>
   store.cart.totalItems;
export const getCartTotalPriceSelector = (store: { cart: ICart }) =>
   store.cart.cartItems.reduce<number>((prev: number, sneaker: ICartItem) => {
      return prev + sneaker.count * sneaker.price;
   }, 0);

export const { addCartItem, updateCartItem, removeCartItem } =
   cartSlice.actions;

export default cartSlice.reducer;