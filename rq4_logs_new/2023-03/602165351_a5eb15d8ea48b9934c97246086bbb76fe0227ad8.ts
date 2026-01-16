import { createSlice } from "@reduxjs/toolkit";
import notificationSlice from "./notification";

const authSlice = createSlice({
  name: "auth",
  initialState: {
    token: null,
  },
  reducers: {
    setToken(state, action) {
      state.token = action.payload.token;
    },
    removeToken(state) {
      state.token = null;
    },
  },
});

export const login =
  ({ email, password }) =>
  async (dispatch) => {
    try {
      let url = process.env.NEXT_PUBLIC_BASE_URL + "auth/login";
      let response = await fetch(url, {
        method: "POST",
        body: JSON.stringify({ email, password }),
        headers: {
          "Content-Type": "application/json",
        },
      });
      response = await response.json();
      dispatch(authSlice.actions.setToken({ token: "some token" }));
    } catch (error: any) {
      console.log(error);
      dispatch(
        notificationSlice.actions.setNotification({
          type: "error",
          message: error.message,
          visible: true,
        })
      );
    }
  };

export default authSlice;