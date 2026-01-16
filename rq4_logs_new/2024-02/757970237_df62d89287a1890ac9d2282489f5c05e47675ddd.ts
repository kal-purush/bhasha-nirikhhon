/* eslint-disable no-param-reassign */
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { Phone } from 'src/types/Phone';
import { getPhones } from '../api/phones';

interface PhonesState {
  phones: Phone[];
  loading: boolean;
  error: string;
}

const initialState: PhonesState = {
  phones: [],
  loading: false,
  error: '',
};

export const init = createAsyncThunk('phones/fetch', () => getPhones());

const phonesSlice = createSlice({
  name: 'phones',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(init.pending, (state) => {
      state.loading = true;
    });

    builder.addCase(init.fulfilled, (state, action) => {
      state.loading = false;
      state.phones = action.payload;
    });

    builder.addCase(init.rejected, (state) => {
      state.loading = false;
      state.error = 'Something went wrong';
    });
  },
});

export default phonesSlice.reducer;