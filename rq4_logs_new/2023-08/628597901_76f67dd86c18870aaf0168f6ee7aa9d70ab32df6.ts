/* import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { Track } from "../../components/trackApi";

interface controlsSlice {
  currentTracks: Track[];
}

const initialState: controlsSlice = {
  currentTracks: [],
};

const controlsSlice = createSlice({
  name: "tracks",
  initialState,
  reducers: {
    setCurrentTracks(
      state,
      action: PayloadAction<Track[] | any[] | undefined>
    ) {
      if (action.payload !== undefined) {
        state.currentTracks = action.payload;
      }
    },
  },
});
export const { setCurrentTracks } = controlsSlice.actions;
export default controlsSlice.reducer;
 */