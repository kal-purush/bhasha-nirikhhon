import { Reducer } from "redux";
import { ActionWithPayload } from "./reducer";
import { ErrorVisualizerState, initialErrorVisualizerState } from "./state.type";

export const setSharedDataReducer: Reducer<ErrorVisualizerState, ActionWithPayload<any[]>> = (state = initialErrorVisualizerState, action) => {
  if (action.type != "" || !action.payload)
    return state;

  console.warn(action.payload);

  return {
    ...state,
    errors: [...state.errors, ...action.payload],
  };
};