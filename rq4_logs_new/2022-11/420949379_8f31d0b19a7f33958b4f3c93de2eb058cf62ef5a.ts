import { createSlice } from '@reduxjs/toolkit';
import type { PayloadAction } from '@reduxjs/toolkit';

import { Nullable, requestStatusType } from 'types';

export interface appReducerInitialStateType {
  status: requestStatusType;
  errorLog: Nullable<string>;
  isInitialized: boolean;
}

const initialState: appReducerInitialStateType = {
  status: 'idle',
  errorLog: null,
  isInitialized: false,
};

export const appSlice = createSlice({
  name: 'app',
  initialState,
  reducers: {
    setAppStatus: (state, action: PayloadAction<requestStatusType>) => {
      state.status = action.payload;
    },
    setErrorLog: (state, action: PayloadAction<Nullable<string>>) => {
      state.errorLog = action.payload;
    },
    setIsInitialized: (state, action: PayloadAction<boolean>) => {
      state.isInitialized = action.payload;
    },
  },
});

export const appReducerRtk = appSlice.reducer;

// Action creators are generated for each case reducer function
export const { setAppStatus, setErrorLog, setIsInitialized } = appSlice.actions;