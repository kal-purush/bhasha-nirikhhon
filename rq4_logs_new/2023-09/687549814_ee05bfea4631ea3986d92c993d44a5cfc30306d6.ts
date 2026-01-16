import { createSlice } from "@reduxjs/toolkit";
import { PaginationItem, PictureItem } from "./types";
import { fetchItems } from "../thunks/items/itemsThunks";

export interface ItemsState {
  data: PictureItem[];
  pagination: PaginationItem;
}

const initialState: ItemsState = {
  data: [],
  pagination: {
    current_page: 1,
    limit: 9,
    offset: 0,
    total: 0,
    total_pages: 0,
  },
};

export const itemsSlice = createSlice({
  name: "items",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(
      fetchItems.fulfilled,
      (state, action: { payload: ItemsState; type: string }) => {
        state.data = action.payload.data;
        state.pagination = action.payload.pagination;
      }
    );
  },
});

export default itemsSlice.reducer;