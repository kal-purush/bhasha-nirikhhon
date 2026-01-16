import { 
    ADD_PRODUCT_TO_CART
} from './../constants/ActionContants';


const initialState = {
    cartItems: [],
    isBuyItemActive: false
};

const cartState = (
    state = initialState,
    action: { type: string; payload: any}
) => {
    switch(action.type) {

        case ADD_PRODUCT_TO_CART: 
            console.log("From Cart.reducer" + state);
            return { ...state, cartItems: [...state.cartItems, action.payload]}

        default:
            return state;
    }
}

export default cartState;