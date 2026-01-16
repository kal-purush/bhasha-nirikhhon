import { createSlice } from "@reduxjs/toolkit";

type PaginationDataType = {
  totalItems: number;
  totalPages: number;
  currentPage: number;
};
interface paginationDataState {
  paginationData: PaginationDataType;
}

const initialState: paginationDataState = {
  paginationData: {
    totalItems: 0,
    totalPages: 0,
    currentPage: 0,
  },
};

const paginationSlice = createSlice({
  name: "paginationData",
  initialState: initialState,
  reducers: {
    setTotalFoundJobs(state, action) {
      state.paginationData = action.payload;
    },
  },
});

export const { setTotalFoundJobs } = paginationSlice.actions;

export default paginationSlice.reducer;