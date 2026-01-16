/* External dependencies */
import { createAsyncThunk, createSlice } from '@reduxjs/toolkit';
import { $api, getRequest } from '../../pages/api/axois-api';

export const getSubscriptionById: any = createAsyncThunk(
  'subscribtion-by-id',
  async (token: string, { rejectWithValue }) => {
    try {
      const response = await getRequest(token, 'v1/tracks'); //https://api.kodjaz.com/api/v1/tracks/

      console.log(response);

      return response.data;
    } catch (error: any) {
      return rejectWithValue(error);
    }
  },
);

interface userSubscriptionByIdState {
  error?: Error;
  loading: boolean;
  lesson?: '';
}

const initialState: userSubscriptionByIdState = {
  loading: false,
};

const userSubscriptionByIdSlice = createSlice({
  name: 'subscribtion-by-id',
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder
      .addCase(getSubscriptionById.pending, (state) => {
        state.loading = true;
      })
      .addCase(getSubscriptionById.fulfilled, (state, { payload }) => {
        state.lesson = payload;
      })
      .addCase(getSubscriptionById.rejected, (state, { payload }) => {
        state.error = payload;
      });
  },
});

export default userSubscriptionByIdSlice.reducer;