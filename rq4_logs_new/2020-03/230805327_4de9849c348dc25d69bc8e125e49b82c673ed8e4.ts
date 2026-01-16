import {
  configureStore,
  combineReducers,
  Action,
  createSlice,
  PayloadAction
} from "@reduxjs/toolkit";
import { ThunkAction } from "redux-thunk";
import { LoadingState } from "./types";
import { API_URL } from "./constants";
import { delay } from "./utils";

// Hot Loading setup
//@ts-ignore
if (process.env.NODE_ENV === "development" && module.hot) {
  //@ts-ignore
  module.hot.accept("./duck", () => {
    const newRootReducer = require("./duck").rootReducer;
    store.replaceReducer(newRootReducer);
  });
}

interface CoreState {
  loadingState: LoadingState;
  schools: { [s: string]: string };
  error: string | undefined;
}

const initialState: CoreState = {
  loadingState: LoadingState.Loading,
  schools: {},
  error: undefined
};

const coreSlice = createSlice({
  name: "core",
  initialState,
  reducers: {
    getSchoolsSuccess(state, action: PayloadAction<{ [s: string]: string }>) {
      state.loadingState = LoadingState.Success;
      state.schools = action.payload;
    },
    getSchoolsFailure(state, action: PayloadAction<string>) {
      state.error = action.payload;
    },
    finishLoading(state) {
      state.loadingState = LoadingState.Success;
    }
  }
});

const {
  getSchoolsSuccess,
  getSchoolsFailure,
  finishLoading
} = coreSlice.actions;

export const getSchools = (): AppThunk => async (dispatch, getState) => {
  const {
    core: { schools }
  } = getState();
  if (Object.entries(schools).length === 0) {
    // Just to mess with Albert
    await delay(500 + Math.random() * 500);
    try {
      const res = await fetch(`${API_URL}/schools`);
      const payload = await res.json();
      const schools: { [s: string]: string } = {};
      for (const school of payload) {
        schools[school.code] = school.name;
      }
      dispatch(getSchoolsSuccess(schools));
    } catch (e) {
      dispatch(getSchoolsFailure(e.toString()));
    }
  } else {
    dispatch(finishLoading());
  }
};

export { finishLoading };

export const rootReducer = combineReducers({
  core: coreSlice.reducer
});

export type RootState = ReturnType<typeof rootReducer>;
export const store = configureStore({
  reducer: rootReducer
});
export type AppThunk = ThunkAction<void, RootState, unknown, Action<string>>;
export type AppDispatch = typeof store.dispatch;

export default store;