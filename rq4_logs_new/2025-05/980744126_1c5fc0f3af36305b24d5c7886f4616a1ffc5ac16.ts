import { UserType } from "@/interfaces/user-interface";
import { createSlice } from "@reduxjs/toolkit";

const userSlice = createSlice({
  name: "user",
  initialState: {
    currentUser: null,
    currentUserId: "",
  },
  reducers: {
    SetCurrentUser: (state, action) => {
      state.currentUser = action.payload;
    },
    SetCurrentUserId: (state, action) => {
      state.currentUserId = action.payload;
    },
  },
});

export const { SetCurrentUser, SetCurrentUserId } = userSlice.actions;
export default userSlice;

export interface UserStateType {
  currentUser: UserType | null;
  currentUserId: string;
}