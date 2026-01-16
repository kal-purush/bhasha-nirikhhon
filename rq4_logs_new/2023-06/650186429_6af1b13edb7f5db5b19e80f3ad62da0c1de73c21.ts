import AuthState from "<@>/types/auth-state";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";


const initialState: AuthState = {
  token: null,
  isAuthenticated: false,
  role: null, // Initialize role to null
};

const authSlice = createSlice({
  name: "auth",
  initialState,
  reducers: {
    setToken(state, action: PayloadAction<AuthState>) {
      state.token = action.payload.token;
      state.isAuthenticated = true;
      state.role = action.payload.role; 
      if (action.payload.token) {
        localStorage.setItem("token", JSON.stringify(action.payload.token));
              }
      if (action.payload.role) {
        localStorage.setItem("role", JSON.stringify(action.payload.role));
              }
    },
    clearToken(state) {
      state.token = null;
      state.isAuthenticated = false;
      state.role = null; // Clear role
      localStorage.removeItem("token");
      localStorage.removeItem("role"); // Remove role from localStorage
    }
  },
});

export const { setToken, clearToken } = authSlice.actions;
export default authSlice.reducer;