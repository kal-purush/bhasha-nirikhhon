import { fetchSearchArtworks } from "./../thunks/search/searchThunks";
import { SearchItem } from "./searchTypes";
import { createSlice } from "@reduxjs/toolkit";

export interface SearchState {
  data: SearchItem[];
}

const initialState: SearchState = {
  data: [],
};

export const searchSlice = createSlice({
  name: "search",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(
      fetchSearchArtworks.fulfilled,
      (state, action: { payload: SearchState; type: string }) => {
        state.data = action.payload.data;
      }
    );
  },
});

export default searchSlice.reducer;