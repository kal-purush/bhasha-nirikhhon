import { configureStore } from "@reduxjs/toolkit";
import userReducer from "./UserSlice";
const appStore = configureStore({
  reducer: {
    User: userReducer,
  },
});
export default appStore;