import { createSlice } from '@reduxjs/toolkit';
import { RootState } from '@store';

export interface ThemeState {
  mode: 'LIGHT' | 'DARK';
}

const initialState: ThemeState = {
  mode: 'LIGHT',
};

const themeSlice = createSlice({
  name: 'theme',
  initialState,
  reducers: {
    changeTheme: (state) => {
      state.mode = state.mode == 'LIGHT' ? 'DARK' : 'LIGHT';
    },
  },
});

export const { changeTheme } = themeSlice.actions;

export const selectCurrentTheme = (state: RootState) => state.theme.mode;

export default themeSlice.reducer;