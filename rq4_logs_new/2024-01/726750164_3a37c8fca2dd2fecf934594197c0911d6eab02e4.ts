import { createSlice } from "@reduxjs/toolkit";

interface FilterState {
  searchKeyword: string;
  location: string;
  position: string;
  salaryRange: string;
  experienceRange: string;
  field: string;
  major: string;
  workMode: string;
  isHot: boolean;
}

const initialState: FilterState = {
  searchKeyword: "",
  location: "0",
  position: "0",
  salaryRange: "0",
  experienceRange: "0",
  field: "0",
  major: "0",
  workMode: "0",
  isHot: false,
};

const jobFilterSlice = createSlice({
  name: "jobFilter",
  initialState,
  reducers: {
    setJobFilter: (state, action) => {
      const {
        searchKeyword,
        location,
        position,
        salaryRange,
        experienceRange,
        field,
        major,
        workMode,
        isHot,
      } = action.payload;

      // Update the state with the new filter values
      state.searchKeyword = searchKeyword;
      state.location = location;
      state.position = position;
      state.salaryRange = salaryRange;
      state.experienceRange = experienceRange;
      state.field = field;
      state.major = major;
      state.workMode = workMode;
      state.isHot = isHot;
    },
  },
});

export const { setJobFilter } = jobFilterSlice.actions;

export default jobFilterSlice.reducer;