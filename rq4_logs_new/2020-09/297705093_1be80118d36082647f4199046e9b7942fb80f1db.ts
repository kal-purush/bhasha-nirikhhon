import {Action} from 'redux';
import {ThunkAction} from 'redux-thunk';

import axios from '../../helpers/axios.config';

import {RootState} from '../rootReducer';
import {authLoginAction} from './auth.actions';
import {SignIn, SignUp} from '../../types/auth';

export function authLogin(
  data: SignIn,
): ThunkAction<void, RootState, unknown, Action<string>> {
  return async (dispatch) => {
    const res = await axios.post('/login', data);
    console.log(res);
  };
}

export function authRegister(
  data: SignUp,
): ThunkAction<void, RootState, unknown, Action<string>> {
  return async (dispatch) => {
    const res = await axios.post('/register', data);
    console.log(res);
  };
}