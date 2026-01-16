import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { notify } from "@/helpers";
import { IUser } from "@/models/auth";
import { login, register } from "@/services/auth";

export const _login = createAsyncThunk(
  `login`,
  async (payload: { email: string; password: string }, thunkApi) => {
    try {
      const response = await login(payload);
      return response;
    } catch (error) {
      return thunkApi.rejectWithValue(error);
    }
  }
);

export const _register = createAsyncThunk(
  `register`,
  async (
    payload: { email: string; password: string; userName: string },
    thunkApi
  ) => {
    try {
      const response = await register(payload);
      return response;
    } catch (error) {
      return thunkApi.rejectWithValue(error);
    }
  }
);

interface authState {
  token: string | null;
  user: IUser | null;
  loading: boolean;
  error: boolean;
  invalidSession: boolean;
}

const initialState: authState = {
  token: null,
  user: null,
  loading: false,
  error: false,
  invalidSession: false,
};

export const auth = createSlice({
  name: "auth",
  initialState,
  reducers: {
    logOut: (state) => {
      state.token = null;
      state.user = null;
      state.loading = false;
      state.error = false;
      state.invalidSession = false;
    },
    invalidSession: (state) => {
      state.invalidSession = true;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(_login.pending, (state) => {
        state.token = null;
        state.user = null;
        state.loading = true;
        state.error = false;
        state.invalidSession = false;
      })
      .addCase(_login.rejected, (state) => {
        state.token = null;
        state.user = null;
        state.loading = false;
        state.error = true;
        state.invalidSession = false;
      })
      .addCase(_login.fulfilled, (state, { payload }) => {
        state.token = payload.data.data.token;
        state.user = payload.data.data.data;
        state.loading = false;
        state.error = false;
        state.invalidSession = false;
        notify(payload.data.message, "success", "logged-in");
      });
    builder
      .addCase(_register.pending, (state) => {
        state.token = null;
        state.user = null;
        state.loading = true;
        state.error = false;
        state.invalidSession = false;
      })
      .addCase(_register.rejected, (state) => {
        state.token = null;
        state.user = null;
        state.loading = false;
        state.error = true;
        state.invalidSession = false;
      })
      .addCase(_register.fulfilled, (state, { payload }) => {
        state.token = payload.data.data.token;
        state.user = payload.data.data.data;
        state.loading = false;
        state.error = false;
        state.invalidSession = false;
        notify(payload.data.message, "success", "logged-in");
      });
  },
});

export const { logOut, invalidSession } = auth.actions;
export default auth.reducer;