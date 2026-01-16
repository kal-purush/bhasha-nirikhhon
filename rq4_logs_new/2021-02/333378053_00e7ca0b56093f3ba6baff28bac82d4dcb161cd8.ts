import { Item, CartActionTypes, ADD_ITEM_TO_CART } from './types'

export function addItemToCart(newItem: Item): CartActionTypes {
  return {
    type: ADD_ITEM_TO_CART,
    payload: newItem,
  }
}