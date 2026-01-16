import {createAction, props} from "@ngrx/store";
import {Product} from "../models/product.model";

export const addProduct = createAction('[Product] Add Product', props<{product: Product}>());
export const addProductSuccess = createAction('[Product] Add Product Success');
export const addProductFailure = createAction('[Product] Add Product Failure', props<{error: string}>());

export const getAllProducts = createAction('[Product] Get All Products');
export const getAllProductsSuccess = createAction('[Product] Get All Products Success', props<{products: Product[]}>());
export const getAllProductsFailure = createAction('[Product] Get All Products Failure', props<{error: string}>());

export const deleteProduct = createAction('[Product] Delete Product', props<{product: Product}>());
export const deleteProductSuccess = createAction('[Product] Delete Product Success');
export const deleteProductFailure = createAction('[Product] Delete Product Failure', props<{error: string}>());

export const updateProduct = createAction('[Product] Update Product', props<{product: Product}>());
export const updateProductSuccess = createAction('[Product] Update Product Success');
export const updateProductFailure = createAction('[Product] Update Product Failure', props<{error: string}>());

export const getById = createAction('[Product] Get Product By Id', props<{id: string}>());
export const getByIdSuccess = createAction('[Product] Get Product By Id Success', props<{product: Product}>());
export const getByIdFailure = createAction('[Product] Get Product By Id Failure', props<{error: string}>());