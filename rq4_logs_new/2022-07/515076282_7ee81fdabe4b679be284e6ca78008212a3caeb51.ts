import { createAction, props } from '@ngrx/store';
import { Product } from 'src/app/data/product';

export const addProduct = createAction(
  '[Add Product Form] Add product',
  props<{ product: Product }>()
);

export const getProducts = createAction('[Product List] Load products');

export const getProductsSuccess = createAction(
  '[API] Get Products',
  props<{ products: Product[] }>()
);

export const updateProduct = createAction(
  '[Edit Product Form] Edit product',
  props<{ product: Product }>()
);

export const deleteProduct = createAction(
  '[Product List] Delete product',
  props<{ productId: number }>()
);