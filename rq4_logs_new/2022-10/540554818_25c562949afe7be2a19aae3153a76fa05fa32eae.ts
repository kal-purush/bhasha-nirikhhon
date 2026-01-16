import { createSlice, PayloadAction } from '@reduxjs/toolkit';

import { RootState } from '..';
import { AlphabetLetters, IInitialStateApp } from '../../types/AppSlice';

const initialState: IInitialStateApp = {
  guessWord: 'виселица',
  currentWord: 'в сел ца',
  wrongLetters: ['д', 'р', 'ш', 'к'],
  successLetters: ['а', 'в', 'л', 'с', 'ц'],
};

export const getAppState = (state: RootState) => state.app;

const app = createSlice({
  name: 'app',
  initialState,
  reducers: {
    saveGuessWord(state, { payload }: PayloadAction<string>) {
      state.guessWord = payload;
    },

    addWrongLetter(state, { payload }: PayloadAction<AlphabetLetters>) {
      state.wrongLetters.push(payload);
    },
    addSuccessLetter(state, { payload }: PayloadAction<AlphabetLetters>) {
      state.successLetters.push(payload);
    },
  },
});

export const { saveGuessWord } = app.actions;

export default app.reducer;