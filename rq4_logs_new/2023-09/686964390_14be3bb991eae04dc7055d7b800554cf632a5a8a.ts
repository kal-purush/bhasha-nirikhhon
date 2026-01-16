import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { User } from "../../components/types/type";

type InitialState = {
  users: User[];
  isLoading: boolean;
  searchUser: [];
};

const initialState: InitialState = {
  users: [],
  isLoading: true,
  searchUser: [],
};

const userSlice = createSlice({
  name: "users",
  initialState,
  reducers: {
    getUserData: (state, action: PayloadAction<User[]>) => {
      state.users = action.payload;

      state.isLoading = false;
    },

    SearchUsers: (state, action: PayloadAction<string>) => {
      const result = state.users.filter((item) =>
        item.email.toLowerCase().includes(action.payload.toLowerCase())
      );
      state.users = result;
    },
  },
});
export const UserActions = userSlice.actions;

const userReducer = userSlice.reducer;

export default userReducer;