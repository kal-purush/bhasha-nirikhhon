import createDataContext from '@utils/createDataContext';
import {cloneDeep} from 'lodash';

//MARK: action type
export const fetch_data_price = 'fetch_data_price';
export const order_data_products = 'order_data_products';
export const order_data_ingredients = 'order_data_ingredients';
export const clear_data = 'clear_data';

//MARK: init state
export const initPrice = {
  totalPrice: 0,
  orderProduct: [] as any,
  orderIngredient: [] as any,
};

export type TInitPrice = typeof initPrice;

//MARK: Reducer
export const reducer = (state: TInitPrice = initPrice, action: any) => {
  switch (action.type) {
    case fetch_data_price:
      return {
        ...state,
      };

    case order_data_products:
      let product = [...state.orderProduct];
      const index = product.findIndex(
        item => item.productTemplateId === action.payload.productTemplateId,
      );
      if (index !== -1) {
        product[index] = action.payload;
      } else {
        product.push(action.payload);
      }
      return {
        ...state,
        orderProduct: product,
        price: action.payload.price,
      };
    case order_data_ingredients:
      const ingredient = [...state.orderIngredient];
      const _index = ingredient.findIndex(
        item => item.id === action.payload.id,
      );
      if (_index !== -1) {
        ingredient[_index] = action.payload;
      } else {
        ingredient.push(action.payload);
      }
      return {
        ...state,
        orderIngredient: ingredient,
      };

    case clear_data:
      return {
        ...initPrice,
      };
    default:
      return initPrice;
  }
};

//MARK: actions
export const orderProduct = (dispatch: any) => (payload: any) => {
  dispatch({type: order_data_products, payload});
};

export const orderIngredient = (dispatch: any) => (payload: any) => {
  dispatch({type: order_data_ingredients, payload});
};

export const clearData = (dispatch: any) => () => {
  dispatch({type: clear_data});
};

//MARK: selectors
export const calculateTotalPriceSelector = (
  product: TInitPrice['orderProduct'],
  ingredient: TInitPrice['orderIngredient'],
) => {
  const totalProduct = product.reduce(
    (sum: number, _product: TInitPrice['orderProduct']) => {
      return sum + _product.price;
    },
    0,
  );

  const totalIngredient = ingredient.reduce(
    (sum: number, _ingredient: TInitPrice['orderIngredient']) => {
      return sum + _ingredient.price;
    },
    0,
  );

  return totalIngredient + totalProduct;
};

export const getListCart = (
  product: TInitPrice['orderProduct'],
  ingredient: TInitPrice['orderIngredient'],
) => {
  const listCart = product.map((item: any) => {
    return {
      productTemplateId: item.productTemplateId,
      quantity: item.quantity,
      ingredients: ingredient.filter(
        (i: any) => i.productId === item.productTemplateId,
      ),
    };
  });

  return listCart;
};

export const {Provider, Context} = createDataContext({
  reducer,
  actions: {
    orderProduct,
    orderIngredient,
    clearData,
  },
  selectors: {
    calculateTotalPriceSelector,
    getListCart,
  },
  initialState: cloneDeep(initPrice),
});