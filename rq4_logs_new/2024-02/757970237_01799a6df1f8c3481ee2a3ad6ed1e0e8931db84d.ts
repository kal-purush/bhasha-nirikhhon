/* eslint-disable no-param-reassign */
import { createSlice } from '@reduxjs/toolkit';

type Theme = 'light' | 'dark';

const initialState: { theme: Theme } = {
  theme: 'light',
};

const themeSlice = createSlice({
  name: 'theme',
  initialState,
  reducers: {
    changeTheme: (state) => {
      if (state.theme === 'light') {
        state.theme = 'dark';
      } else {
        state.theme = 'light';
      }
    },
  },
});

export default themeSlice.reducer;
export const { actions } = themeSlice;