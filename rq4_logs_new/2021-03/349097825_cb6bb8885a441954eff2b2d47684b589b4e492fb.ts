import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import type { AppThunk, RootState } from 'app/store';
import { IWord, IWords, IWordDifficulty, WordsList } from '../../types';
import {
  api,
  COULD_NOT_GET_WORDS,
  GET_WORDS_API,
  SERVER_OK_STATUS,
} from '../../constants';

const initialState: IWords = {
  data: [],
  loaded: false,
  loadError: undefined,
};

export const wordsSlice = createSlice({
  name: 'words',
  initialState,
  reducers: {
    setWordsLoadedStatus: (state, action: PayloadAction<boolean>) => {
      state.loaded = action.payload;
    },
    setWordsLoadError: (state, action: PayloadAction<string | undefined>) => {
      state.loadError = action.payload;
    },
    setWords: (state, action: PayloadAction<Array<IWord>>) => {
      state.data = action.payload;
    },
    setWordDifficulty: (state, action: PayloadAction<IWordDifficulty>) => {
      const index = state.data.findIndex((el) => String(el.id) === action.payload.wordId);
      if (index > -1) {
        state.data[index].difficulty = action.payload.difficulty;
      }
    },
  },
});

export const loadWords = (sectorNum: number, pageNum: number): AppThunk => async (
  dispatch
) => {
  dispatch(setWordsLoadedStatus(true));
  try {
    dispatch(setWordsLoadError(undefined));

    const params = new URLSearchParams([
      ['page', `${pageNum}`],
      ['group', `${sectorNum}`],
    ]);

    const options = {
      method: 'GET',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json; charset=UTF-8',
      },
    };

    const response = await fetch(`${api}/${GET_WORDS_API}?${params}`, options);

    if (response.status !== SERVER_OK_STATUS) {
      dispatch(setWordsLoadError(COULD_NOT_GET_WORDS));
    } else {
      const data = (await response.json()) as WordsList;
      dispatch(setWords(data));
    }
  } catch (e) {
    dispatch(setWordsLoadError(e.message));
  }
  dispatch(setWordsLoadedStatus(false));
};

export const {
  setWords,
  setWordsLoadedStatus,
  setWordsLoadError,
  setWordDifficulty,
} = wordsSlice.actions;

export const getWords = (state: RootState) => state.words.data;

export const getWordsLoadErrMessage = (state: RootState) => state.words.loadError;

export const getWordsLoadedStatus = (state: RootState) => state.words.loaded;

export default wordsSlice.reducer;