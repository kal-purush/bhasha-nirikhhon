import { createSlice } from "@reduxjs/toolkit";

export interface HistoryState {
  historyArts: string[];
}

const initialState: HistoryState = {
  historyArts: [],
};

export const artworksSlice = createSlice({
  name: "history",
  initialState,
  reducers: {
    addHistoryItem(state, action) {
      state.historyArts = [...state.historyArts, action.payload];
    },
    removeHistoryItem(state, action) {
      state.historyArts = state.historyArts.filter(
        (item) => item !== action.payload
      );
    },
    clearHistory(state) {
      state.historyArts = [];
    },
  },
});

export const { addHistoryItem, removeHistoryItem, clearHistory } =
  artworksSlice.actions;

export default artworksSlice.reducer;