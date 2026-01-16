import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import * as t from 'types';
import { RootState } from '../../app/store';

interface SettingsState {
  value: t.Settings;
}

const initialState: SettingsState = {
  value: {
    translation: true,
    buttons: true,
  },
};

export const settingsSlice = createSlice({
  name: 'settings',
  initialState,
  reducers: {
    changeSettings: (state, action: PayloadAction<t.Settings>) => {
      state.value = action.payload;
    },
  },
});

export const { changeSettings } = settingsSlice.actions;

export const selectSettings = (state: RootState) => state.settings.value;
export default settingsSlice.reducer;