import {createAction, props} from "@ngrx/store";
import {Product} from "../../models/product.model";
import {Cart} from "../../models/cart.model";

export const addNewCart = createAction('[Cart] Create New Cart', props<{cart: Cart}>());
export const addNewCartSuccess = createAction('[Cart] Create New Cart Success');
export const addNewCartFailure = createAction('[Cart] Create New Cart Failure', props<{error: string}>());

export const addProductToCart = createAction('[Cart] Add Product To Cart', props<{cartId: string, product: Product, quantity: number}>());
export const addProductToCartSuccess = createAction('[Cart] Add Product To Cart Success');
export const addProductToCartFailure = createAction('[Cart] Add Product To Cart Failure', props<{error: string}>());