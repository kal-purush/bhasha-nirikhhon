import {createSlice} from '@reduxjs/toolkit';
import {NameSpace} from '../../constants';
import {CommentData} from '../../types/state';
import {fetchComments, postComment} from '../api-actions';

const initialState: CommentData = {
  comments: [],
  isCommentsDataLoading: false,
  hasError: false,
  hasErrorPost: false
};


export const commentsData = createSlice({
  name: NameSpace.DataComments,
  initialState,
  reducers: {},
  extraReducers(builder) {
    builder
      .addCase(fetchComments.pending, (state) => {
        state.isCommentsDataLoading = true;
        state.hasError = false;
      })
      .addCase(fetchComments.fulfilled, (state, action) => {
        state.comments = action.payload;
        state.isCommentsDataLoading = false;
      })
      .addCase(fetchComments.rejected, (state) => {
        state.isCommentsDataLoading = false;
        state.hasError = true;
      })
      .addCase(postComment.fulfilled, (state, action) => {
        state.comments.push(action.payload);
        state.hasErrorPost = false;
      })
      .addCase(postComment.rejected, (state) => {
        state.hasErrorPost = true;
      });
  }
});
