import { createSlice } from "redux-starter-kit";

const initialState: { token: null | string; deviceId: null | string } = {
  token: null,
  deviceId: null
};

const authSlice = createSlice({
  initialState,
  slice: "auth",
  reducers: {
    setToken(state, action) {
      const { token } = action.payload;
      state.token = token;
    },
    setDeviceId(state, action) {
      const { deviceId } = action.payload;
      state.deviceId = deviceId;
    }
  }
});

export const { setToken, setDeviceId } = authSlice.actions;
export const authReducer = authSlice.reducer;