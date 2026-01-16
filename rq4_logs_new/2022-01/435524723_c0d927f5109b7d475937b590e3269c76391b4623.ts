import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { AuthResponse } from '../../models/auth';

import { RootState } from '../store';

interface AuthState {
  credentials?: AuthResponse;
}

const initialState: AuthState = {
  credentials: undefined,
}

export const authSlice = createSlice({
  name: 'auth',
  initialState,
  reducers: {
    setCredentials: (state: AuthState, action: PayloadAction<AuthResponse | undefined>) => {
      state.credentials = action.payload;
    },
  },
});

export const { setCredentials } = authSlice.actions;

export const selectCredentials = (state: RootState): AuthResponse | undefined => state.auth.credentials;

export const isAuthenticated = (state: RootState): boolean => state.auth.credentials !== undefined;

export default authSlice.reducer;