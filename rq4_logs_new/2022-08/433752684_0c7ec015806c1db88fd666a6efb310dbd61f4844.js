import { createSlice } from "@reduxjs/toolkit";
import { fetchHelpSubCategories } from "./services/helpSubCategories";

const initialState = {
  helpArticles: [],
  loading: false,
};

export const helpSubCategoriesSlice = createSlice({
  name: "helpSubCategory",
  initialState,
  reducers: {
    handleLoading: (state, action) => {
      state.loading = action.payload;
    },
  },
  extraReducers: (builder) => {
    builder.addCase(fetchHelpSubCategories.fulfilled, (state, action) => {
      state.helpArticles = action.payload;
      state.loading = false;
    });
  },
});

export const { handleLoading } = helpSubCategoriesSlice.actions;

export default helpSubCategoriesSlice.reducer;