import { createSlice } from "@reduxjs/toolkit";

const initialState = {
  addTaskPopupOpen: false,
  updateTaskPopup: {},
};

const togglePopupSlice = createSlice({
  name: "togglePopup",
  initialState,
  reducers: {
    openPopup: (state, { payload }) => {
      const { popupType, isOpen, taskId } = payload;
      if (taskId) {
        if (!state.updateTaskPopup[taskId]) {
          state.updateTaskPopup[taskId] = false;
        }
        state.updateTaskPopup[taskId] = true;
      } else {
        state[`${popupType}PopupOpen`] = isOpen;
      }
    },
    closePopup: (state) => {
      state.addTaskPopupOpen = false;
      state.updateTaskPopup = {};
    },
  },
});

export const { openPopup, closePopup } = togglePopupSlice.actions;
export const popupReducer = togglePopupSlice.reducer;