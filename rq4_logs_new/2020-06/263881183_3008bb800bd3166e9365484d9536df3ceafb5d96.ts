import { createSlice, PayloadAction } from '@reduxjs/toolkit';

import { RootState } from '../';
import { CalendarState } from './CalendarTypes';

const initialState: CalendarState = {
  selectedDay: null,
  isEditMode: false,
};

const calendarSlice = createSlice({
  name: 'calendar',
  initialState,
  reducers: {
    setIsEditMode(state, action: PayloadAction<boolean>) {
      state.isEditMode = action.payload;
    },
    setSelectedDay(state, action: PayloadAction<CalendarState['selectedDay']>) {
      state.selectedDay = action.payload;
    },
  },
});

export const { setIsEditMode, setSelectedDay } = calendarSlice.actions;
export const calendarSelectors = {
  selectIsEditMode: (state: RootState) => state.calendar.isEditMode,
  selectedSelectedDay: (state: RootState) => state.calendar.selectedDay,
};

export default calendarSlice.reducer;