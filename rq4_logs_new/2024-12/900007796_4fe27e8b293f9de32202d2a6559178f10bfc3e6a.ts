import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { FiltersType } from "../../../shared/types/types";

const initialState: FiltersType = {
  category: null,
  keywords: null,
};

const filtersSlice = createSlice({
  name: "filters",
  initialState,
  reducers: {
    changeCategory(state, action: PayloadAction<string>) {
      if (action.payload === "All") {
        state.category = null;
      } else {
        state.category = action.payload;
      }
    },
    changeKeywords(state, action: PayloadAction<string>) {
      if (action.payload === "") {
        state.keywords = null;
      } else {
        state.keywords = action.payload;
      }
    },
  },
});

export const { changeCategory, changeKeywords } = filtersSlice.actions;

export const filtersReducer = filtersSlice.reducer;