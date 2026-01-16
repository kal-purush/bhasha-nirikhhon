import { createSlice, PayloadAction } from '@reduxjs/toolkit';

import { ErrorMessage } from '../../models/error';
import { RootState } from '../store';

interface ErrorHandlerState {
  errorMessage?: ErrorMessage;
}

const initialState: ErrorHandlerState = {
  errorMessage: undefined,
}

export const errorHandlerSlice = createSlice({
  name: 'errorHandler',
  initialState,
  reducers: {
    setError: (state: ErrorHandlerState, action: PayloadAction<any>) => {
      state.errorMessage = action.payload;
    },
  },
});

export const { setError } = errorHandlerSlice.actions;

export const selectError = (state: RootState): ErrorMessage | undefined => state.errorHandler?.errorMessage;

export default errorHandlerSlice.reducer;