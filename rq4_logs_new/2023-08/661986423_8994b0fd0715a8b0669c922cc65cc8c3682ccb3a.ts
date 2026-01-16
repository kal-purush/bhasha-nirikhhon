import { createSlice } from '@reduxjs/toolkit';
import { TComments } from '../../types/state';
import { NameSpace } from '../../const';
//import { fetchCommentsOfferAction } from '../api-action';
import { toast } from 'react-toastify';
import { fetchCommentsOfferAction, postCommentOfferAction } from '../api-action';

const initialState: TComments = {
  comments: [],
  isCommentsDataLoading: false,
};

export const comments = createSlice({
  name: NameSpace.Comment,
  initialState,
  reducers: {},
  extraReducers(builder) {
    builder
      .addCase(fetchCommentsOfferAction.pending, (state) => {
        state.isCommentsDataLoading = true;
      })
      .addCase(fetchCommentsOfferAction.fulfilled, (state, action) => {
        state.comments = action.payload;
        state.isCommentsDataLoading = false;
      })
      .addCase(fetchCommentsOfferAction.rejected, (state) => {
        state.isCommentsDataLoading = false;
      })
      .addCase(postCommentOfferAction.fulfilled, (state, action) => {
        state.comments.unshift(action.payload);
      })
      .addCase(postCommentOfferAction.rejected, () => {
        toast.warn('Failed to post comment. Please, try again later');
      });
  }
});