import { createSlice, Dispatch } from "@reduxjs/toolkit";
import { authProvider } from "../../utils";
import { persistReducer } from "redux-persist";
import storage from "redux-persist/lib/storage";

const initialState: {
  isAuthenticated: boolean;
  loading: boolean;
} = {
  isAuthenticated: false,
  loading: false,
};

const slice = createSlice({
  name: "auth",
  initialState,
  reducers: {
    reset() {
      return initialState;
    },
    showLoading(state) {
      state.loading = true;
    },
    hideLoading(state) {
      state.loading = false;
    },
    setSession(state) {
      state.isAuthenticated = true;
    },
  },
});

export function login(email: string, password: string) {
  return (dispatch: Dispatch<any>) => {
    try {
      dispatch(slice.actions.showLoading());
      const result = authProvider.login(email, password);
      dispatch(slice.actions.hideLoading());
      dispatch(slice.actions.setSession());

      return result;
    } catch (e) {
      dispatch(slice.actions.hideLoading());
      return null;
    }
  };
}

const persistConfig = {
  key: "auth",
  storage,
  keyPrefix: "redux-",
  whitelist: ["isAuthenticated", "profile"],
};

const { reset, setSession } = slice.actions;

export { reset as resetAuth, setSession };
export default persistReducer(persistConfig, slice.reducer);