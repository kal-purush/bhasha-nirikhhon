import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { BoardType } from "../../types";
import type { RootState } from "../store";

interface BorderState {
  boards: BoardType[];
  loading: boolean;
}

const initialState: BorderState = {
  boards: [],
  loading: true,
};

export const getBoards = createAsyncThunk("boards/getBoards", async () => {
  //mvoe this to service
  const response = await fetch(import.meta.env.VITE_API_URL + "/board");
  const data = await response.json();
  return data;
});

export const boardSlice = createSlice({
  name: "boards",
  initialState,
  reducers: {},
  extraReducers: {
    [getBoards.pending.type]: (state, action) => {
      state.loading = true;
    },
    [getBoards.fulfilled.type]: (state, action) => {
      state.boards = action.payload;
      state.loading = false;
    },
    [getBoards.rejected.type]: (state, action) => {
      state.loading = false;
    },
  },
});

export const {} = boardSlice.actions;

export default boardSlice.reducer;