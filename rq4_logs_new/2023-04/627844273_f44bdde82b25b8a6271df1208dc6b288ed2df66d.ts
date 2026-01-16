import { createSlice } from "@reduxjs/toolkit";
import { RootState } from "../store";
import { IUiState } from "./types";

const initialState: IUiState = {
  bottomNav: false,
  theme: "light",
};

export const uiSlice = createSlice({
  name: "ui",
  initialState,
  reducers: {
    toggleBottomNav: (state) => {
      state.bottomNav = !state.bottomNav;
    },
  },
});

// ZT-NOTE: Action creators exports
export const { toggleBottomNav } = uiSlice.actions;

// ZT-NOTE: Selector funtions exports for multiple react components to use
export const isShowBottomNav = (state: RootState) => state.ui.bottomNav;

export default uiSlice.reducer;