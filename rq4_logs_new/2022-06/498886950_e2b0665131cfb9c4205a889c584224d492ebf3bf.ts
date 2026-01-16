import { CartState } from '..';
import { ICartProduct } from '../../interfaces/cart';

type CartActionTypes =
  | {
      type:
        | '[Cart] Load card from cookies | storage'
        | '[Cart] Update cart'
        | '[Cart] Clear cart';
      payload: ICartProduct[];
    }
  | {
      type:
        | '[Cart] Change cart product quantity'
        | '[Cart] Remove cart product';
      payload: ICartProduct;
    }
  | {
      type: '[Cart] Update order summary';
      payload: {
        numberOfItems: number;
        subTotal: number;
        tax: number;
        total: number;
      };
    };

export const cartReducer = (
  state: CartState,
  action: CartActionTypes
): CartState => {
  switch (action.type) {
    case '[Cart] Load card from cookies | storage':
      return { ...state, cart: action.payload };
    case '[Cart] Update cart':
      return {
        ...state,
        cart: action.payload,
      };
    case '[Cart] Change cart product quantity':
      return {
        ...state,
        cart: state.cart.map((product) => {
          if (product._id !== action.payload._id) return product;
          if (product.size !== action.payload.size) return product;
          return action.payload;
        }),
      };

    case '[Cart] Remove cart product':
      return {
        ...state,
        cart: state.cart.filter(
          (product) =>
            !(
              product._id === action.payload._id &&
              product.size === action.payload.size
            )
        ),
      };

    case '[Cart] Update order summary':
      return {
        ...state,
        ...action.payload,
      };
    default:
      return state;
  }
};