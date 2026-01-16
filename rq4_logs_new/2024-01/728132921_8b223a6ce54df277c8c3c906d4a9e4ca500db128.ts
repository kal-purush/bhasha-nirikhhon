import { PayloadAction, createSlice } from "@reduxjs/toolkit";

export interface ModalState {
  isOpen: boolean;
}

const initialState: ModalState = {
  isOpen: false,
};

export const modalSlice = createSlice({
  name: "modal",
  initialState,
  reducers: {
    setIsOpen: (state, action: PayloadAction<boolean>) => {
      state.isOpen = action.payload;
    },
  },
});

export const modalActions = modalSlice.actions;

const modalReducer = modalSlice.reducer;

export default modalReducer;