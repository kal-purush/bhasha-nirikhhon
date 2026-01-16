// contains action-related type aliases, interfaces and so on
// Package imports
import { AnyAction } from "redux";
import { ThunkDispatch } from "redux-thunk";
// Non-package imports
import { ActionTypes } from "./types";
import { RootState } from "../reducers";
import { Toy, User, ErrorObject } from "appTypes";

// Error-related interfaces
export interface ReturnErrorsAction {
  type: ActionTypes.RETURN_ERRORS;
  payload: ErrorObject;
}

export interface ClearErrorsAction {
  type: ActionTypes.CLEAR_ERRORS;
  payload?: undefined;
}

// Loader-related interfaces
export interface ShowLoaderAction {
  type: ActionTypes.ACTION_SHOW_LOADER;
  payload?: {
    // the prop name is unknown and not fixed, but the type of the key is known
    [showLoader: string]: boolean;
  };
}

// Toys-related interfaces
export interface GetToyListSuccessAction {
  type: ActionTypes.GET_TOY_LIST_SUCCESS;
  payload: Toy[] | [];
}
export interface GetToyListFailAction {
  type: ActionTypes.GET_TOY_LIST_FAIL;
  payload?: undefined;
}
export interface GetToySuccessAction {
  type: ActionTypes.GET_TOY_SUCCESS;
  payload: Toy;
}
export interface GetToyFailAction {
  type: ActionTypes.GET_TOY_FAIL;
  payload?: undefined;
}
export interface CreateToySuccessAction {
  type: ActionTypes.CREATE_TOY_SUCCESS;
  payload: Toy;
}
export interface CreateToyFailAction {
  type: ActionTypes.CREATE_TOY_FAIL;
  payload?: undefined;
}
export interface EditToySuccessAction {
  type: ActionTypes.EDIT_TOY_SUCCESS;
  payload: Toy;
}
export interface EditToyFailAction {
  type: ActionTypes.EDIT_TOY_FAIL;
  payload?: undefined;
}
export interface DeleteToySuccessAction {
  type: ActionTypes.DELETE_TOY_SUCCESS;
  payload: { toyId: string };
}
export interface DeleteToyFailAction {
  type: ActionTypes.DELETE_TOY_FAIL;
  payload?: undefined;
}
export interface UploadToySuccessAction {
  type: ActionTypes.UPLOAD_TOY_IMAGE_SUCCESS;
  payload: Toy;
}
export interface UploadToyFailAction {
  type: ActionTypes.UPLOAD_TOY_IMAGE_FAIL;
  payload?: undefined;
}
export interface ClearToyListAction {
  type: ActionTypes.CLEAR_TOY_LIST;
  payload?: undefined;
}
export interface CloseToyPageAction {
  type: ActionTypes.CLOSE_TOY_PAGE;
  payload?: undefined;
}

// auth/User-related interfaces
export interface RetrieveUserInfoSuccessAction {
  type: ActionTypes.RETRIEVE_USER_INFO_SUCCESS;
  payload?: User;
}

export interface RetrieveUserInfoFailAction {
  type: ActionTypes.RETRIEVE_USER_INFO_FAIL;
  payload?: undefined;
}

export interface RegisterUserSuccessAction {
  type: ActionTypes.REGISTER_SUCCESS;
  payload?: undefined;
}

export interface RegisterUserFailAction {
  type: ActionTypes.REGISTER_FAIL;
  payload?: undefined;
}

export interface LogoutAction {
  type: ActionTypes.LOGOUT;
  payload?: undefined;
}

// wiki search actions

export interface AddWikiSearchReturnValuesSuccess {
  type: ActionTypes.ADD_WIKI_SEARCH_RETURN_VALUES_SUCCESS;
  payload: any[];
}

export interface AddWikiSearchReturnValuesFail {
  type: ActionTypes.ADD_WIKI_SEARCH_RETURN_VALUES_FAIL;
  payload?: undefined;
}

export interface ClearWikiSearchValues {
  type: ActionTypes.CLEAR_WIKI_SEARCH_VALUES;
  payload?: undefined;
}

// type aliases to be used by the reducers
export type ErrorAction = ReturnErrorsAction | ClearErrorsAction;

export type LoaderAction = ShowLoaderAction;

export type ToysAction =
  | GetToySuccessAction
  | GetToyFailAction
  | GetToyListSuccessAction
  | GetToyListFailAction
  | CreateToySuccessAction
  | CreateToyFailAction
  | EditToySuccessAction
  | EditToyFailAction
  | DeleteToySuccessAction
  | DeleteToyFailAction
  | UploadToySuccessAction
  | UploadToyFailAction
  | ClearToyListAction
  | CloseToyPageAction;

export type AuthAction =
  | RetrieveUserInfoSuccessAction
  | RetrieveUserInfoFailAction
  | RegisterUserSuccessAction
  | RegisterUserFailAction
  | LogoutAction;

export type WikiSearchAction =
  | AddWikiSearchReturnValuesSuccess
  | AddWikiSearchReturnValuesFail
  | ClearWikiSearchValues;

export type FailAction =
  | GetToyFailAction
  | GetToyListFailAction
  | CreateToyFailAction
  | EditToyFailAction
  | DeleteToyFailAction
  | UploadToyFailAction
  | RetrieveUserInfoFailAction
  | RegisterUserFailAction
  | AddWikiSearchReturnValuesFail;

// for useDispatch
export type AppDispatch = ThunkDispatch<RootState, any, AnyAction>;