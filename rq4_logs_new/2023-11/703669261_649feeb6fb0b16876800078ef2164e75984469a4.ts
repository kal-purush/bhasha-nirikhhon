import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

import { RootState } from "../../store/configureStore";

const initialState: any = {
    triggerDelete: false,
    triggerCreate: false,
};

export const patientSlice = createSlice({
    name: "patient",
    initialState,
    reducers: {
        setTriggerDelete: (state, action: PayloadAction<boolean>) => {
            state.triggerDelete = action.payload;
        },
        setTriggerCreate: (state, action: PayloadAction<boolean>) => {
            state.triggerCreate = action.payload;
        },
    },
});

export const { setTriggerDelete, setTriggerCreate } = patientSlice.actions;

export const selectPatient = (state: RootState) => state.patientSlice;

export default patientSlice.reducer;