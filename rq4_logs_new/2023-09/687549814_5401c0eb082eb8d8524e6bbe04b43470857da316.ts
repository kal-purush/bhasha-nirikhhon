import { createSlice } from "@reduxjs/toolkit";

export interface UserState {
  email: string;
  token: string;
  id: string;
  error: string;
}

const initialState: UserState = {
  email: "",
  token: "",
  id: "",
  error: "",
};

const userSlise = createSlice({
  name: "user",
  initialState,
  reducers: {
    setUser(state, action) {
      state.email = action.payload.email;
      state.token = action.payload.token;
      state.id = action.payload.id;
      state.error = "";
    },
    removeUser(state) {
      state.email = "";
      state.token = "";
      state.id = "";
      state.error = "";
    },
    setError(state, action) {
      state.email = "";
      state.token = "";
      state.id = "";
      state.error = action.payload;
    },
  },
});

export const { setUser, removeUser, setError } = userSlise.actions;

export default userSlise.reducer;