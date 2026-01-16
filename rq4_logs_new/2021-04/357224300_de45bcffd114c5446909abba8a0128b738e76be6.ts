import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "../../app/store";

interface PositionState {
  currentPosition: { latitude: number; longitude: number };
  wetherInformation: any;
}

const initialState: PositionState = {
  currentPosition: { latitude: 35.7022589, longitude: 139.7744733 },
  wetherInformation: {},
};

export const positionSlice = createSlice({
  name: "position",
  initialState,

  reducers: {
    setPosition: (state, action) => {
      state.currentPosition.latitude = action.payload.lat;
      state.currentPosition.longitude = action.payload.lng;
    },

    setWetherInformation: (state, action) => {
      state.wetherInformation = action.payload;
    },
  },
});

export const { setPosition, setWetherInformation } = positionSlice.actions;

export const selectPosition = (state: RootState) =>
  state.position.currentPosition;

export const selectCurrentWeather = (state: RootState) =>
  state.position.wetherInformation;

export default positionSlice.reducer;