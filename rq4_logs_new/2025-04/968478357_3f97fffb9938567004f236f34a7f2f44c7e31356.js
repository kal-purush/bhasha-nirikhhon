import { createSlice } from "@reduxjs/toolkit";

const connectionSlice = createSlice({
    name: "connection",
    initialState: {
        connectionData: null
    },
    reducers: {
        setConnectionData: (state, action) => {
            state.connectionData = action.payload;
        },
        removeConnectionData: (state, action) => {
            state.connectionData = null;
        }
    }
})

export const { setConnectionData, removeConnectionData } = connectionSlice.actions;

export default connectionSlice.reducer;