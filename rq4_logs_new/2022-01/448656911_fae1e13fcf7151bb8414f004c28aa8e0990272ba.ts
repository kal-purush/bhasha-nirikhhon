import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';

import { RootState } from '../../app/store';
import { fetchDefinition } from './homeAPI';

export interface HomeState {
  word: string;
  definitions: Definition[];
  loading: boolean;
  error: boolean;
}

const initialState: HomeState = {
  word: '',
  definitions: [],
  loading: false,
  error: false,
};

export const defenitionAsync = createAsyncThunk('fetchDefinition', async (word: string) => {
  const response = await fetchDefinition(word);
  console.log(response.data);
  return response.data;
});

export const homeSlice = createSlice({
  name: 'home',
  initialState,
  reducers: {
    setError: (state, action) => {
      state.error = action.payload;
    },
  },
  extraReducers: (builder) => {
    builder.addCase(defenitionAsync.pending, (state) => {
      state.loading = true;
    });
    builder.addCase(defenitionAsync.fulfilled, (state, action) => {
      state.loading = false;
      state.definitions = action.payload;
    });
    builder.addCase(defenitionAsync.rejected, (state) => {
      state.loading = false;
      state.error = true;
    });
  },
});

export const { setError } = homeSlice.actions;

export const selectDescription = (state: RootState) => state.description;

export default homeSlice.reducer;