import { Action } from 'redux';

export const AuthActions = {
  LOGIN: '[Auth] Login',
  LOGOUT: '[Auth] Logout',
  REGISTER: '[Auth] Register',
};

export interface LoginAction extends Action {
  payload: {
    username: string,
    password: string,
    email: string,
  }
}

export interface LogoutAction extends Action {
  payload: {}
}

export interface RegisterAction extends Action {
  payload: {
    username: string,
    email: string,
    password: string,
  }
}

const login = (username: string, email: string, password: string): LoginAction => ({
  type: AuthActions.LOGIN,
  payload: {
    username,
    email,
    password,
  },
});

const logout = (): LogoutAction => ({
  type: AuthActions.LOGOUT,
  payload: {},
});

const register = (username: string, email: string, password: string): RegisterAction => ({
  type: AuthActions.REGISTER,
  payload: {
    username,
    email,
    password,
  },
});

export default {
  login,
  logout,
  register,
};