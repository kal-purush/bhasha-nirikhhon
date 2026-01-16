import { axiosInstance } from '../../utils';
import { Dispatch } from 'redux';
import { ActionTypes } from './actionTypes';

export interface ProjectDetailType {
  details: string[];
  subTitle: string;
  gitRepoLinks: string[];
  demoLink: string;
  note: string;
}

export interface ProjectType {
  projectDetail: ProjectDetailType;
  img: string;
  title: string;
  description: string;
  id: string;
}

export interface User {
  id: string;
  photo: string;
  role: string;
  intro: string;
  education: string[];
  employmentHistory: string[][];
  skills: string[];
  name: string;
  email: string;
  projects: ProjectType[];
}

interface UserResponse {
  status: string;
  data: User;
}

export interface FetchUserStartAction {
  type: ActionTypes.FETCH_USER_START;
  isLoadingUser: boolean;
  errorMessage: string;
}

export interface FetchUserSuccessAction {
  type: ActionTypes.FETCH_USER_SUCCESS;
  user: User;
  isLoadingUser: boolean;
  errorMessage: string;
}

export interface FetchUserFailAction {
  type: ActionTypes.FETCH_USER_FAIL;
  isLoadingUser: boolean;
  errorMessage: string;
}

export const fetchUser = () => {
  return async (dispatch: Dispatch) => {
    try {
      dispatch<FetchUserStartAction>(fetchUserStart());

      const response = await axiosInstance.get<UserResponse>(
        '/users/5f92374f047d32e5c636c296'
      );

      dispatch<FetchUserSuccessAction>(fetchUserSuccess(response.data.data));
    } catch (err) {
      dispatch<FetchUserFailAction>(fetchUserFail(err.message));
    }
  };
};

const fetchUserStart = (): FetchUserStartAction => {
  return {
    type: ActionTypes.FETCH_USER_START,
    isLoadingUser: true,
    errorMessage: '',
  };
};

const fetchUserSuccess = (user: User): FetchUserSuccessAction => {
  return {
    type: ActionTypes.FETCH_USER_SUCCESS,
    user: user,
    isLoadingUser: false,
    errorMessage: '',
  };
};

const fetchUserFail = (message: string): FetchUserFailAction => {
  return {
    type: ActionTypes.FETCH_USER_FAIL,
    isLoadingUser: false,
    errorMessage: message,
  };
};