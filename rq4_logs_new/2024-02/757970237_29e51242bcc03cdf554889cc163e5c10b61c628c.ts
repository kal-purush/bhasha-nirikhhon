/* eslint-disable no-param-reassign */
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { Product } from '../../types/Product';
import { Category } from '../../types/Category';
import { getAllProducts } from '../../api/products';

interface TabletsState {
  tablets: Product[];
  loading: boolean;
  error: string;
}

const initialState: TabletsState = {
  tablets: [],
  loading: false,
  error: '',
};

export const fetchTablets = createAsyncThunk('tablets/fetch', () =>
  getAllProducts(Category.tablets),
);

const tabletsSlice = createSlice({
  name: 'tablets',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(fetchTablets.pending, (state) => {
      state.error = '';
      state.loading = true;
    });

    builder.addCase(fetchTablets.fulfilled, (state, action) => {
      state.loading = false;
      state.tablets = action.payload;
    });

    builder.addCase(fetchTablets.rejected, (state) => {
      state.loading = false;
      state.error = 'Something went wrong';
    });
  },
});

export default tabletsSlice.reducer;