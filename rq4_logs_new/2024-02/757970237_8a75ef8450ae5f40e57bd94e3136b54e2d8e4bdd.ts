/* eslint-disable no-param-reassign */
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { Category } from '../../types/Category';
import { Product } from '../../types/Product';
import { getProducts } from '../../api/products';

interface ProductsState {
  products: { productsList: Product[]; totalCount: number };
  loading: boolean;
  error: string;
}

const initialState: ProductsState = {
  products: { productsList: [], totalCount: 0 },
  loading: false,
  error: '',
};

export const fetchProducts = createAsyncThunk(
  'products/fetch',
  async (payload: {
    category: Category;
    currentPage?: number;
    perPage?: number;
    sort?: string;
  }) => {
    const { category, currentPage, perPage, sort } = payload;

    return getProducts(category, currentPage, perPage, sort);
  },
);

const productsSlice = createSlice({
  name: 'products',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchProducts.pending, (state) => {
      state.error = '';
      state.loading = true;
    });

    builder.addCase(fetchProducts.fulfilled, (state, action) => {
      state.loading = false;
      state.products = action.payload;
    });

    builder.addCase(fetchProducts.rejected, (state) => {
      state.loading = false;
      state.error = 'Something went wrong';
    });
  },
});

export default productsSlice.reducer;
export const { actions } = productsSlice;