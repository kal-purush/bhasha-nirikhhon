import { createSlice } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";

import { RootState } from "../../store/configureStore";

interface IPractitioner {
    triggerEdit: boolean,

}

const initialState: IPractitioner = {
    triggerEdit: false,

};

export const practitionerSlice = createSlice({
    name: "patient",
    initialState,
    reducers: {
        setTriggerEdit: (state, action: PayloadAction<boolean>) => {
            state.triggerEdit = action.payload;
        },

    },
});

export const { setTriggerEdit } = practitionerSlice.actions;

export const selectPatient = (state: RootState) => state.practitionerSlice;

export default practitionerSlice.reducer;