import { createSlice, Draft } from "@reduxjs/toolkit";
import { Card } from "./../../common/types";

type PostState = {
    selectedPost: Card | null;
}

const initialState: PostState = {
  selectedPost: null,
};

const postsSlice = createSlice({
  name: "posts",
  initialState,
  reducers: {
      setSelectedPost:(state:Draft<PostState>, action) =>{
          state.selectedPost = action.payload
      }
  },
});


export const {setSelectedPost} = postsSlice.actions


export default postsSlice.reducer;


export const PostsSelectors = {
    getSelectedPost:(state:any) => state.post.selectedPost,
}