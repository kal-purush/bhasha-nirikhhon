import { CART_ACTIONS } from './CartAction';
import { 
    ADD_PRODUCT_TO_CART
} from './CartTypes';
import { getProductIndex } from './utils'

export type ProductModel = {
    id: number,
    name: string,
    price: number,
    discountPercentage: number,
    images: string[],
    quantity: number
}

export type CartState ={
    cartItems: ProductModel[],
    isBuyItemActive: boolean
}

const initialState = {
    cartItems: [],
    isBuyItemActive: false
};

const cartState = (
    state: CartState = initialState,
    action: CART_ACTIONS 
) => {
    switch(action.type) {

        case ADD_PRODUCT_TO_CART: {
            const product: ProductModel = action.payload;
            const index = getProductIndex(state.cartItems, product.id);

            if(index == -1) {
                return { ...state, cartItems: [...state.cartItems, action.payload]}
            }

            const currentProduct = state.cartItems[index];
            const updateProduct = Object.assign({}, currentProduct, {quantity: currentProduct.quantity + product.quantity});
            return { ...state, cartItems: [
                ...state.cartItems.slice( 0, index ),
                updateProduct,
                ...state.cartItems.slice( index + 1 ),
            ]}
        }
        default:
            return state;
    }
}

export default cartState;