import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { AuthState, LoginForm, SignupForm } from "../../utils/types";

type ConfigType = {
  breakpoint?: number;
};

const initialState: ConfigType = {
  breakpoint: undefined,
};

const ConfigSlice = createSlice({
  name: "Config",
  initialState,
  reducers: {
    setBreakpoint(state, action: PayloadAction<number>) {
      state.breakpoint = action.payload
    },
  },
});

export const ConfigActions = ConfigSlice.actions;
export const ConfigReducers = ConfigSlice.reducer;
