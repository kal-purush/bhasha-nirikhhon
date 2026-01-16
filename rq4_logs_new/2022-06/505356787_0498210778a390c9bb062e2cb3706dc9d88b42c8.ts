import { createSlice, createAsyncThunk } from "@reduxjs/toolkit";
import axios from "axios";
export const fetchUserById: any = createAsyncThunk(
  "akaBlogSlicerThunk",
  async () => {
    const response = await axios.get("https://fakestoreapi.com/products");
    return response.data;
  }
);

let initialState = {
  data: [],
};

const BlogSlicer = createSlice({
  name: "akaBlogSlicer",
  initialState,
  reducers: {
    blogDataFetchAct: (state: object | any, { payload }: object | any) => {
      state.data = payload;
    },
  },
  extraReducers: {
    [fetchUserById.pending]: () => {
      console.log("pending");
    },
    [fetchUserById.fulfilled]: (state: any, { payload }: any) => {
      console.log("fullfilled ");
      return { ...state, data: payload };
    },
    [fetchUserById.rejected]: () => {
      console.log("rejected");
    },
  },
});
export const { blogDataFetchAct } = BlogSlicer.actions;
export default BlogSlicer.reducer;