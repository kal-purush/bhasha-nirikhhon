import { GET_ORDER } from "../constants/actionTypes";
const initialState = {
  orders: [],
};

const orderReducer = (state = initialState, action) => {
  switch (action.type) {
    case GET_ORDER: {
      return {
        ...state,
        orders: action.payload,
      };
    }
    default: return state;
  }
}

export default orderReducer; 