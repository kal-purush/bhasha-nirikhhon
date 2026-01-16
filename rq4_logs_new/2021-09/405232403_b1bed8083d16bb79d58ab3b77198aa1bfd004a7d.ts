// Reducer related to authentication status
// package import
import { Reducer } from "redux";
// non- package imports
import { ActionTypes, AuthAction } from "../actions";
import { AuthStateObject } from "appTypes";

const INITIAL_STATE: AuthStateObject = {
  isSignedIn: false,
  user: null,
  // the app will try to load the user at first anyway, so may as well set it to true
  isLoading: true,
};

const authReducer: Reducer = (state = INITIAL_STATE, action: AuthAction) => {
  switch (action.type) {
    case ActionTypes.RETRIEVE_USER_INFO_SUCCESS:
      return {
        isSignedIn: true,
        user: action.payload,
        isLoading: true,
      };
    case ActionTypes.REGISTER_SUCCESS:
    case ActionTypes.RETRIEVE_USER_INFO_FAIL:
    case ActionTypes.REGISTER_FAIL:
    case ActionTypes.LOGOUT:
      return {
        isSignedIn: false,
        user: null,
        isLoading: false,
      };
    default:
      return state;
  }
};

export default authReducer;