import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { Topic, Comment } from '../types/types';

interface ForumState {
  topics: Topic[];
  comments: Comment[];
}

const initialState: ForumState = {
  topics: [],
  comments: [],
};

export const forumSlice = createSlice({
  name: 'entities/forum',
  initialState,
  reducers: {
    getTopics(state, action: PayloadAction<Topic[]>) {
      state.topics = action.payload;
    },
  },
});

export const { getTopics } = forumSlice.actions;

export default forumSlice.reducer;