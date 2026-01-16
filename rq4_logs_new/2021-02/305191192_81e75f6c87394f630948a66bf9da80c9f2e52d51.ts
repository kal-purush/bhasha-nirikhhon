import {
    USER_LOADED,
    USER_LOADING,
    AUTH_ERROR,
    LOGIN_SUCCESS,
    LOGIN_FAIL,
    LOGOUT_SUCCESS,
    REGISTER_SUCCESS,
    REGISTER_FAIL
  } from "../actions/types";

interface InitialStateI {
  token: string | null,
  isAuthenticated: boolean | null,
  isLoading: boolean,
  user: boolean |null
}

const initialState: InitialStateI = {
    token: localStorage.getItem("token"),
    isAuthenticated: null,
    isLoading: false,
    user: null
};

export const authReducer = (state: InitialStateI = initialState, action: any): InitialStateI => {
    switch (action.type) {
        case USER_LOADING:
          return {
            ...state,
            isLoading: true
          };
        case USER_LOADED:
          return {
            ...state,
            isAuthenticated: true,
            isLoading: false,
            user: action.payload
          };
    
        case LOGIN_SUCCESS:
        case REGISTER_SUCCESS:
          localStorage.setItem("token", action.payload.token);
          return {
            ...state,
            ...action.payload,
            isAuthenticated: true,
            isLoading: false
          };
    
        case AUTH_ERROR:
        case LOGIN_FAIL:
        case LOGOUT_SUCCESS:
        case REGISTER_FAIL:
          return {
            ...state,
            token: null,
            user: null,
            isAuthenticated: false,
            isLoading: false
          };
    
        default:
          return state;
      }
}