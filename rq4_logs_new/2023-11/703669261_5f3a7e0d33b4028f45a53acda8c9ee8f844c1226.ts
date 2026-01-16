import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

import { RootState } from "../../store/configureStore";

interface IAppointmentState {
  triggerAccept: boolean;
  triggerDeny: boolean;
  triggerNoShow: boolean;
}

const initialState: IAppointmentState = {
  triggerAccept: false,
  triggerDeny: false,
  triggerNoShow: false,
};

export const appointmentSlice = createSlice({
  name: "appointment",
  initialState,
  reducers: {
    setTriggerAccept: (state, action: PayloadAction<boolean>) => {
      state.triggerAccept = action.payload;
    },
    setTriggerDeny: (state, action: PayloadAction<boolean>) => {
      state.triggerDeny = action.payload;
    },
    setTriggerNoShow: (state, action: PayloadAction<boolean>) => {
      state.triggerNoShow = action.payload;
    },
  },
});

export const { setTriggerAccept, setTriggerDeny, setTriggerNoShow } =
  appointmentSlice.actions;

export const selectAppointment = (state: RootState) => state.appointmentSlice;

export default appointmentSlice.reducer;