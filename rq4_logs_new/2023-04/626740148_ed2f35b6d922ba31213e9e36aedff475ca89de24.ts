/** @packages */
import { configureStore } from "@reduxjs/toolkit";
import { createSlice } from "@reduxjs/toolkit";

export const iconslice = createSlice({
  name: "icon",
  initialState: {
    icon: "moon",
  },
  reducers: {
    iconMoon: (state) => {
      state.icon = "moon";
    },
    iconSun: (state) => {
      state.icon = "sun";
    },
  },
});

const store = configureStore({
  reducer: {
    icon: iconslice.reducer,
  },
});

export default store;

export const iconAction = iconslice.actions;