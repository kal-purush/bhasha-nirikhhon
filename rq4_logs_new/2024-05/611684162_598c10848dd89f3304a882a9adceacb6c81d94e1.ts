import { createSlice, PayloadAction } from '@reduxjs/toolkit';

type TagsState = {
  selectedTags: string[];
  isTagListVisible: boolean;
};

const initialState: TagsState = {
  selectedTags: [],
  isTagListVisible: false,
};

const tagsSlice = createSlice({
  name: 'tags',
  initialState,
  reducers: {
    selectTag: (state, action: PayloadAction<string>) => {
      if (state.selectedTags.includes(action.payload)) {
        state.selectedTags = state.selectedTags.filter(tag => tag !== action.payload);
      } else {
        state.selectedTags.push(action.payload);
      }
    },
    resetTags: state => {
      state.selectedTags = [];
    },
    toggleTagListVisibility: state => {
      state.isTagListVisible = !state.isTagListVisible;
    },
  },
});

export const { selectTag, resetTags, toggleTagListVisibility } = tagsSlice.actions;

export default tagsSlice.reducer;