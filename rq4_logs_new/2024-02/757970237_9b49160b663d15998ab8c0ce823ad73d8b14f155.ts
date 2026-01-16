/* eslint-disable no-param-reassign */
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { Product } from '../../types/Product';
import { Category } from '../../types/Category';
import { getAllProducts } from '../../api/products';

interface AccessoriesState {
  accessories: Product[];
  loading: boolean;
  error: string;
}

const initialState: AccessoriesState = {
  accessories: [],
  loading: false,
  error: '',
};

export const fetchAccessories = createAsyncThunk('accessories/fetch', () =>
  getAllProducts(Category.accessories),
);

const accessoriesSlice = createSlice({
  name: 'accessories',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchAccessories.pending, (state) => {
      state.error = '';
      state.loading = true;
    });

    builder.addCase(fetchAccessories.fulfilled, (state, action) => {
      state.loading = false;
      state.accessories = action.payload;
    });

    builder.addCase(fetchAccessories.rejected, (state) => {
      state.loading = false;
      state.error = 'Something went wrong';
    });
  },
});

export default accessoriesSlice.reducer;