// Package imports
import axios, { AxiosResponse, AxiosError } from "axios";
import { Dispatch } from "redux";
// Non-package imports
import serverRest from "api/serverRest";
import cloudinaryRest from "api/cloudinaryRest";
import history from "../../browserHistory";
import { Toy } from "appTypes";
import { clearErrors } from "./errorActions";
import { actionShowLoader } from "./loaderActions";
import { errorHandler, renderNotification } from "helpers";
import { ActionTypes } from "./types";
import { RootState } from "../reducers/index";

export interface ToyListQuery {
  filter?: string | string[] | null;
  sortPropName?: string | string[] | null;
  sortOrder?: string | string[] | null;
}

export const getToy =
  (toyId: string) =>
  (dispatch: Dispatch): void => {
    serverRest
      .get(`/toys/${toyId}`)
      .then((res: AxiosResponse) => {
        let toy: Toy = res.data;
        dispatch({
          type: ActionTypes.GET_TOY_SUCCESS,
          payload: toy,
        });
        dispatch(clearErrors());
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        errorHandler(dispatch, err, { type: ActionTypes.GET_TOY_FAIL });
      });
  };

export const getToyList =
  ({ sortPropName, sortOrder, filter }: ToyListQuery) =>
  (dispatch: Dispatch) => {
    // assemble the query string conditionally to avoid errors
    const queryString = `${
      sortPropName ? `sortPropName=${sortPropName}` : ""
    }&${sortOrder ? `sortOrder=${sortOrder}` : ""}&${
      filter ? "filter=" + filter : ""
    }`;
    serverRest
      .get(`/toys?${queryString || ""}`)
      .then((res: AxiosResponse) => {
        let toys: Toy[] = res.data;
        console.log(toys);
        dispatch({
          type: ActionTypes.GET_TOY_LIST_SUCCESS,
          payload: toys,
        });
        dispatch(clearErrors());
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        errorHandler(dispatch, err, { type: ActionTypes.GET_TOY_LIST_FAIL });
      });
  };

export const createToy =
  (formValues: Toy, successCb?: Function) =>
  (dispatch: Dispatch, getState: () => RootState) => {
    const userId = getState().auth.user.id;
    // send a POST request to the server, passing in the form values as the request body
    serverRest
      .post(`/toys/`, { ...formValues, userId })
      .then((res: AxiosResponse) => {
        const toy: Toy = res.data;
        // pass the updated user and toy payload to the reducer, change Redux store state, clear errors, run any callbacks,
        dispatch({
          type: ActionTypes.CREATE_TOY_SUCCESS,
          payload: toy,
        });
        console.log(toy);
        dispatch(clearErrors());
        history.push(`/toys/${toy.id || toy._id}`);
        if (successCb) successCb();
        renderNotification({
          message: "Successfully created a toy page!",
          type: "success",
        });
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        errorHandler(dispatch, err, { type: ActionTypes.CREATE_TOY_FAIL });
      })
      .finally((): void => {
        dispatch(actionShowLoader("createToyForm", false));
      });
  };

export const editToy =
  (toyId: string | undefined, formValues: Toy, successCb?: Function) =>
  (dispatch: Dispatch, getState: () => RootState) => {
    const userId = getState().auth.user.id;
    serverRest
      .patch(`/toys/${toyId}`, { ...formValues, userId })
      .then((res: AxiosResponse) => {
        const toy: Toy = res.data;
        // change Redux store state, and pass the updated user and toy payload, then run any callbacks on success
        dispatch({
          type: ActionTypes.EDIT_TOY_SUCCESS,
          payload: toy,
        });
        // clear any errors on the form and run any success callbacks
        dispatch(clearErrors());
        if (successCb) successCb();
        renderNotification({
          message: "Successfully updated toy page.",
          type: "success",
        });
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        console.log(err);
        console.log(JSON.stringify(err));
        errorHandler(dispatch, err, { type: ActionTypes.EDIT_TOY_FAIL });
      })
      .finally((): void => {
        dispatch(actionShowLoader("editToyForm", false));
      });
  };

export const deleteToy =
  (toyId: string, successCb?: Function) =>
  (dispatch: Dispatch, getState: () => RootState) => {
    const userId = getState().auth.user.id;
    console.log(toyId);
    axios
      .delete(
        `http://localhost:5000/api/toys/${toyId}`,
        //required when sending data using delete method
        { data: { userId } }
      )
      .then((res: AxiosResponse) => {
        dispatch({
          type: ActionTypes.DELETE_TOY_SUCCESS,
          payload: {
            ...res.data,
          },
        });
        dispatch(clearErrors());
        if (successCb) successCb();
        history.push("/toys");
        renderNotification({
          message: "Successfully deleted the toy.",
          type: "info",
        });
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        errorHandler(dispatch, err, { type: ActionTypes.DELETE_TOY_FAIL });
      })
      // remove the loader
      .finally((): void => {
        dispatch(actionShowLoader("deleteToyForm", false));
      });
  };

export const uploadToyImage =
  (base64EncodedImage: any, toyId: String, successCb?: Function) =>
  (dispatch: Dispatch, getState: () => RootState) => {
    const userId = getState().auth.user.id;
    cloudinaryRest
      .patch(
        `/api/toys/${toyId}/upload_image`,
        JSON.stringify({ fileStr: base64EncodedImage, userId })
      )
      .then((res) => {
        const toy = res.data;
        dispatch({
          type: ActionTypes.UPLOAD_TOY_IMAGE_SUCCESS,
          payload: toy,
        });
        renderNotification({
          message: "Successfully uploaded toy image.",
          type: "success",
        });
        if (successCb) successCb();
      })
      // handle failure by logging and displaying a visual indicator
      .catch((err: AxiosError) => {
        errorHandler(dispatch, err, {
          type: ActionTypes.UPLOAD_TOY_IMAGE_FAIL,
        });
      })
      // remove the loader
      .finally((): void => {
        dispatch(actionShowLoader("uploadToyImageForm", false));
      });
  };