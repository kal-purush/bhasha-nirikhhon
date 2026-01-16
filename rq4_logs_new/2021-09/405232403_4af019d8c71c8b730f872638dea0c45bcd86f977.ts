// Package imports
import { ActionCreator, AnyAction } from "redux";
// Non-package imports
import { ActionTypes } from "./types";

// RETURN ERRORS
//TODO: change this structure if needed
//TODO: add interface for return values
export const returnErrors: ActionCreator<AnyAction> = (
  message: {} | string,
  status: null | string = null,
  id: null | string = null
) => {
  console.error(message);
  return {
    type: ActionTypes.RETURN_ERRORS,
    payload: { message, status, id },
  };
};

// CLEAR ERRORS
export const clearErrors: ActionCreator<AnyAction> = () => {
  return {
    type: ActionTypes.CLEAR_ERRORS,
  };
};