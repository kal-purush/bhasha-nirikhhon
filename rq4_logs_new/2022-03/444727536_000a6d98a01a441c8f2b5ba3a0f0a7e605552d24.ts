import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  modalType: null,
  isModalOpen: false,
  modalProps: {},
};

const modalSlice = createSlice({
  name: "groupModal",
  initialState,
  reducers: {
    showModal: (state: any, { payload }: any) => {
      state.modalType = payload.modalType;
      state.isModalOpen = payload.isModalOpen;
      state.modalProps = payload.modalProps;
    },
    closeModal: () => {
      return initialState;
    },
  },
});

export const { showModal, closeModal } = modalSlice.actions;

export const modalReducer = modalSlice.reducer;