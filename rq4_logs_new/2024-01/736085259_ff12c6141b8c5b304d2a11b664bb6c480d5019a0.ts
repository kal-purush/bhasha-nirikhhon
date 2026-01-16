import { createSlice, createAsyncThunk } from "@reduxjs/toolkit";
import { Employee, HrEmployeeProfileData } from "./hrTypes";
import axios from "axios";

export const fetchEmployeeProfiles = createAsyncThunk(
  "hr/fetchEmployeeProfiles",
  async () => {
    const response = await axios.get(
      "http://localhost:3000/hr/getEmployeeProfiles"
    );
    return response.data as Employee[]; 
  }
);

interface EmployeeState {
  employees: Employee[];
}

const initialState: EmployeeState = {
  employees: [],
  // initial values for other properties
};

const hrSlice = createSlice({
  name: "hr",
  initialState,
  reducers: {
    // other synchronous reducers
  },
  extraReducers: (builder) => {
    builder.addCase(fetchEmployeeProfiles.fulfilled, (state, action) => {
      state.employees = action.payload;
    });
    // Handle pending and rejected states as needed
  },
});

export default hrSlice.reducer;