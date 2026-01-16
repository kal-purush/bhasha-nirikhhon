import { createSlice } from "@reduxjs/toolkit";
import { PictureItem } from "../artworks/types";

export interface FavoriteState {
  favoriteArts: PictureItem[];
}

const initialState: FavoriteState = {
  favoriteArts: [],
};

export const artworksSlice = createSlice({
  name: "favorite",
  initialState,
  reducers: {
    addFavoriteItem(state, action) {
      state.favoriteArts.push(action.payload);
    },
    removeFavoriteItem(state, action) {
      state.favoriteArts = state.favoriteArts.filter(
        ({ id }) => id !== action.payload
      );
    },
    clearFavorites(state) {
      state.favoriteArts = [];
    },
  },
});

export const { addFavoriteItem, removeFavoriteItem, clearFavorites } =
  artworksSlice.actions;

export default artworksSlice.reducer;