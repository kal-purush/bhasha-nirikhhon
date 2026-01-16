import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { IUser } from "shared";

export interface AuthState {
  loggedIn: Boolean;
  name?: string;
  email?: string;
}

const initialState: AuthState = {
  loggedIn: false
};

const authSlice = createSlice({
  name: "auth",
  initialState: initialState,
  reducers: {
    logIn: (state, action: PayloadAction<IUser>) => {
      state.loggedIn = true
      state.name = action.payload.name
      state.email = action.payload.email
    }
  },
});

export const { logIn } = authSlice.actions;
export default authSlice;