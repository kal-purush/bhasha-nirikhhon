import { createSlice } from "@reduxjs/toolkit";

interface StudentsState {
  students: object[] | null;
}

const initialState: StudentsState = {
  students: null,
};

const studentsSlice = createSlice({
  name: "students",
  initialState,
  reducers: {
    setStudents: (state, action) => {
      state.students = action.payload;
    },
  },
});

export const { setStudents } = studentsSlice.actions;
export default studentsSlice.reducer;