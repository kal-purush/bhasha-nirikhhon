import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import type { AppThunk, RootState } from 'app/store';
import { IUser, IUserLogInData } from '../../types';
import { api, LOG_IN_API } from '../../constants';

const initialState: IUser = {
  token: '',
  refreshToken: '',
  userId: '',
  name: '',
};

export const userSlice = createSlice({
  name: 'user',
  initialState,
  reducers: {
    setUser: (state, action: PayloadAction<IUser>) => {
      state.token = action.payload.token;
      state.refreshToken = action.payload.refreshToken;
      state.userId = action.payload.userId;
      state.name = action.payload.name;
    },
    unsetUser: (state) => {
      state.token = '';
      state.refreshToken = '';
      state.userId = '';
      state.name = '';
    },
  },
});

export const logIn = (logInData: IUserLogInData): AppThunk => async (dispatch) => {
  try {
    const options = {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=UTF-8' },
      body: JSON.stringify({ ...logInData }),
    };
    const response = await fetch(`${api}/${LOG_IN_API}`, options);

    console.log(response);
    /*
    if (response.status !== 201) {
      const body = (await response.json()) as IResponse;

      const errors: ILogInErrors = {
        general: body.message,
        login: null,
        password: null,
      };

      if (body.errors && body.errors.length) {
        // eslint-disable-next-line promise/prefer-await-to-callbacks
        body.errors.forEach((error) => {
          if (Object.keys(errors).includes(error.param)) {
            errors[error.param] = error.msg;
          }
        });
      }
      dispatch(logInFailure(errors));
    } else {
      const user = (await response.json()) as IUser;
      dispatch(logInSuccess(user));
      const lsItem: string | null = localStorage.getItem(LOCALSTORAGE_KEY);
      const savedUserData = lsItem ? (JSON.parse(lsItem) as IUser) : {};

      localStorage.setItem(
        LOCALSTORAGE_KEY,
        JSON.stringify({
          ...savedUserData,
          ...user,
        })
      );
    } */
  } catch (e) {
    console.log(e);
    /* if (e instanceof Error) {
      const errors: ILogInErrors = {
        general: e.message,
        login: null,
        password: null,
      };
      dispatch(logInFailure(errors));
    } */
  }
  dispatch(setUser({ token: '', refreshToken: '', userId: '', name: '' }));
};

export const { setUser, unsetUser } = userSlice.actions;

export const getCurrUser = (state: RootState) => state.user;

export default userSlice.reducer;