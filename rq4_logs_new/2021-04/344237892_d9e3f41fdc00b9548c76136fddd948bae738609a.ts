import * as types from './auth.types';
import { ThunkAction } from 'redux-thunk';
import { api } from 'src/api';
import { RootStateType } from '../rootReducer';
import { IUser } from './auth.types';

export type ThunkType<ReturnType = void> = ThunkAction<
  ReturnType,
  RootStateType,
  unknown,
  types.AuthActionTypes
>;

export const fetchRequestAuth = (): types.AuthActionTypes => {
  return {
    type: types.FETCH_REQUEST_AUTH,
  };
};

export const updateToken = (access_token: string): types.AuthActionTypes => ({
  type: types.FETCH_SUCCESS_TOKEN,
  payload: access_token,
});

export const updateUser = (user: IUser): types.AuthActionTypes => ({
  type: types.FETCH_SUCCESS_AUTH,
  payload: user,
});

export const fetchAuth = (access_token: string): ThunkType => async (
  dispatch
) => {
  try {
    const result = await api.get('/users', {
      headers: {
        Authorization: `Bearer ${access_token}`,
      },
    });
    console.log('result: ', result);
    dispatch(updateUser(result.data));
    dispatch(fetchTasks(access_token));
  } catch (error) {
    dispatch({
      type: types.FETCH_ERROR_AUTH,
      payload: error.message,
    });
  }
};

export const fetchAuthOnLogin = ({
  email,
  password,
}: types.IUserAuth): ThunkType => async (dispatch) => {
  try {
    const result = await api.post('/auth/login', {
      email,
      password,
    });
    const {
      tokenData: { access_token },
      userData,
    } = result.data;
    window.localStorage.setItem('access_token', access_token);
    dispatch(updateToken(access_token));
    dispatch(updateUser(userData));
    dispatch(fetchTasks(access_token));
  } catch (error) {
    console.log(error.message);
    dispatch({
      type: types.FETCH_ERROR_AUTH,
      payload: error.message,
    });
  }
};

export const updateTasks = (tasks: any): types.AuthActionTypes => {
  return {
    type: types.UPDATE_TASKS,
    payload: tasks,
  };
};

export const updateUserTasks = (userTasks: any): types.AuthActionTypes => {
  return {
    type: types.UPDATE_USER_TASKS,
    payload: userTasks,
  };
};

export const fetchTasks = (access_token: string): ThunkType => async (
  dispatch
) => {
  const result = await api.get('/tasks', {
    headers: {
      Authorization: `Bearer ${access_token}`,
    },
  });
  dispatch(updateTasks(result.data));
};

export const fetchUserTasks = ({ access_token, userId }): ThunkType => async (
  dispatch
) => {
  const result = await api.get(`/tasks/developer/${userId}`, {
    headers: {
      Authorization: `Bearer ${access_token}`,
    },
  });
  dispatch(updateUserTasks(result.data));
};

export const onLogOut = (): types.AuthActionTypes => {
  return {
    type: types.LOGOUT,
  };
};