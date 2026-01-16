import { JobDetailType } from "@data/interface";
import { createSlice } from "@reduxjs/toolkit";

interface JobDetailState {
  jobDetail: JobDetailType | null;
}

const initialState: JobDetailState = {
  jobDetail: null,
};

const jobDetailSlice = createSlice({
  name: "jobDetail",
  initialState: initialState,
  reducers: {
    setJobDetail(state, action) {
      state.jobDetail = action.payload;
    },
  },
});

export const { setJobDetail } = jobDetailSlice.actions;

export default jobDetailSlice.reducer;