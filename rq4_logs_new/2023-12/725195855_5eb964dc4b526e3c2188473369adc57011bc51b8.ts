import { createSlice } from '@reduxjs/toolkit';

export type ModalState = {
  isActive: boolean;
};

const initialState = {
  isActive: false,
} as ModalState;

export const modal = createSlice({
  name: 'modal',
  initialState,
  reducers: {
    setActive: (state) => {
      state.isActive = true;
    },
    setInactive: (state) => {
      state.isActive = false;
    },
  },
});

export const { setActive, setInactive } = modal.actions;

export default modal.reducer;