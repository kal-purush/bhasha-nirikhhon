import axios, { AxiosResponse } from "axios";
import { Dispatch } from "redux";

import { BASE_URL } from "action";
import { IAction, IResponse } from "model/action";
import { IAuthor, IAuthorFormValues } from "model/author";
import { IState } from "model/state";
import { getCurrentUserId } from "selector/user";

export const CURRENT_AUTHOR_PARSE = "AUTHOR_PARSE";
export const AUTHOR_LIST_PARSE = "AUTHOR_LIST_PARSE";

const END_POINT_URL = "authors/";

export const parseCurrentAuthor = (item: IAuthor): IAction<IAuthor> => (
  { type: CURRENT_AUTHOR_PARSE, payload: item }
);

export const parseAuthorList = (list: IAuthor[]): IAction<IAuthor[]> => (
  { type: AUTHOR_LIST_PARSE, payload: list }
);

export const loadAuthor = (id: number) => {
  return (dispatch: Dispatch<IState>): Promise<void> => {
    return axios.get(`${BASE_URL}${END_POINT_URL}${id}`).then(
      (response: AxiosResponse<IResponse<IAuthor>>) => {
        dispatch(parseCurrentAuthor(response.data.data));
      },
    );
  };
};

export const loadAuthorList = () => {
  return (dispatch: Dispatch<IState>): Promise<void> => {
    return axios.get(`${BASE_URL}${END_POINT_URL}`).then(
      (response: AxiosResponse<IResponse<IAuthor[]>>) => {
        dispatch(parseAuthorList(response.data.data));
      },
    );
  };
};

export const createAuthor = (formData: IAuthorFormValues) => {
  return (dispatch: Dispatch<IState>, getState: () => IState): Promise<void> => {
    const userId = getCurrentUserId(getState());
    return axios.post(`${BASE_URL}${END_POINT_URL}`, { ...formData, userId }).then(
      (response: AxiosResponse<IResponse<IAuthor[]>>) => {
        dispatch(loadAuthorList());
      },
    );
  };
};

export const updateAuthor = (formData: IAuthorFormValues, id: number) => {
  return (dispatch: Dispatch<IState>, getState: () => IState): Promise<void> => {
    const userId = getCurrentUserId(getState());
    return axios.patch(`${BASE_URL}${END_POINT_URL}${id}`, { ...formData, userId }).then(
      (response: AxiosResponse<IResponse<IAuthor[]>>) => {
        dispatch(loadAuthorList());
      },
    );
  };
};

export const deleteAuthor = (id: number) => {
  return (dispatch: Dispatch<IState>, getState: () => IState): Promise<void> => {
    const userId = getCurrentUserId(getState());
    return axios.put(`${BASE_URL}${END_POINT_URL}${id}`, { userId }).then(
      (response: AxiosResponse<IResponse<IAuthor[]>>) => {
        dispatch(loadAuthorList());
      },
    );
  };
};