import { Contest } from "<@>/types/Journey/Contest";
import { PayloadAction, createSlice } from "@reduxjs/toolkit";

export interface SelectedContest {
  id: string | null;
}
const initialState: SelectedContest = { id: null };

const contestSlice = createSlice({
  name: "selectedContest",
  initialState,
  reducers: {
    setSelectedContest(state, action: PayloadAction<SelectedContest>) {
      return action.payload;
    },
  },
});

export const { setSelectedContest } = contestSlice.actions;
export default contestSlice.reducer;