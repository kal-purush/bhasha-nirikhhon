import { createSlice } from "@reduxjs/toolkit";

interface CompletedQuizzesState {
  completedQuizzes: object[] | null;
}

const initialState: CompletedQuizzesState = {
  completedQuizzes: null,
};

const completedQuizzesSlice = createSlice({
  name: "completedQuizzes",
  initialState,
  reducers: {
    setCompletedQuizzes: (state, action) => {
      state.completedQuizzes = action.payload;
    },
  },
});

export const { setCompletedQuizzes } = completedQuizzesSlice.actions;
export default completedQuizzesSlice.reducer;