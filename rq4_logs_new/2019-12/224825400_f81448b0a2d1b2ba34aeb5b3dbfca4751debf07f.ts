import { configureStore, createSlice } from "@reduxjs/toolkit";

const buttonSlice = createSlice({
  name: "button",
  initialState: false,
  reducers: {
    click() {
      return true;
    }
  }
});

const store = configureStore({
  reducer: buttonSlice.reducer
});

export const { click } = buttonSlice.actions;

export default store;