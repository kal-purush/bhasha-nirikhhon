import type { CartItem } from '../../types/database'
import type { CustomAction } from '../../types/redux'

export enum CartActionTypes {
  ADD_CART_ITEM = 'ADD_CART_ITEM',
  INCREASE_CART_ITEM = 'INCREASE_CART_ITEM',
  DECREASE_CART_ITEM = 'DECREASE_CART_ITEM',
  REMOVE_CART_ITEM = 'REMOVE_CART_ITEM'
}

export const CartItemAdd = (CartItemData: Omit<CartItem, 'id'> & Partial<Pick<CartItem, 'id'>>): CustomAction<CartItem> => ({
  type: CartActionTypes.ADD_CART_ITEM,
  payload: {
    body: CartItemData
  }
})

export const CartItemIncrease = (id: CartItem['id']): CustomAction<CartItem> => ({
  type: CartActionTypes.INCREASE_CART_ITEM,
  payload: {
    id,
  }
})

export const CartItemDecrease = (id: CartItem['id']): CustomAction<CartItem> => ({
  type: CartActionTypes.DECREASE_CART_ITEM,
  payload: {
    id,
  }
})

export const CartItemRemove = (id: CartItem['id']): CustomAction<CartItem> => ({
  type: CartActionTypes.REMOVE_CART_ITEM,
  payload: {
    id
  }
})