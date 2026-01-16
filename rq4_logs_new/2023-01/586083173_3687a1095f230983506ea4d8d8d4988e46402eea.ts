import { createSlice } from '@reduxjs/toolkit';
import { Device } from '@store/index/index.props';

const initialState: Device = {
    token: null
};

export const DeviceReducer = createSlice({
    name: 'device',
    initialState,
    reducers: {
        setDeviceTokenAction: (state, action) => {
            state.token = action.payload;
        }
    }
});

export const { setDeviceTokenAction } = DeviceReducer.actions;

export default DeviceReducer.reducer;