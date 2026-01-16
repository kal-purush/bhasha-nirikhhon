// package imports
import axios, { AxiosResponse, AxiosError } from "axios";
import { Dispatch } from "redux";
// non- package imports
import serverRest from "api/serverRest";
import history from "browserHistory";
import { clearErrors } from "./errorActions";
import { actionShowLoader } from "./loaderActions";
import { errorHandler, renderNotification } from "helpers";

// List of action types to be used
//TODO:  check what I really need to keep
import { ActionTypes } from "./types";

// After logging in or retrieving current user, retrieve additional information from the database
export const retrieveUserInfo = (username: string) => (dispatch: Dispatch) => {
  serverRest
    .get(`/users/${username}`)
    .then((res: AxiosResponse) => {
      dispatch({
        type: ActionTypes.RETRIEVE_USER_INFO_SUCCESS,
        payload: res.data,
      });
    })
    .catch((err: AxiosError) => {
      errorHandler(dispatch, err, {
        type: ActionTypes.RETRIEVE_USER_INFO_FAIL,
      });
    });
};

// Register User (returned username from cognito)
export const registerUser = (username: string) => (dispatch: Dispatch) => {
  serverRest
    .post("/users/register", { username })
    .then((res: AxiosResponse) => {
      dispatch({ type: ActionTypes.REGISTER_SUCCESS });
      dispatch(clearErrors());
      // redirect to validation page
      history.push("/validate-email");
    })
    .catch((err: AxiosError) => {
      // this needs an error handler action creator and reducer
      errorHandler(dispatch, err, { type: ActionTypes.REGISTER_FAIL });
    })
    .finally(() => {
      dispatch(actionShowLoader("registerForm", false));
    });
};

// Logout User
export const logoutUser = () => (dispatch: Dispatch) => {
  dispatch({
    type: ActionTypes.LOGOUT,
  });
  dispatch(clearErrors());
  history.push("/login");
  renderNotification({
    message: "Successfully logged out.",
    type: "info",
    position: "top-center",
  });
};