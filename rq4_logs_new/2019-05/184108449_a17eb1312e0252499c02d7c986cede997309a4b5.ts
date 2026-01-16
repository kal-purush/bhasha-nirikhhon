import { Action } from '@ngrx/store';
import { IUser } from './auth.reducer';

export enum AuthActionTypes {
  LoadAuth = '[Auth] Load Auth',
  AnonymousLogin = '[Auth] Anonymous Login',
  GoogleLogin = '[Auth] Google Login',
  AuthLoaded = '[Auth] Auth Loaded',
  AuthLoadError = '[Auth] Auth Load Error'
}

export class LoadAuth implements Action {
  readonly type = AuthActionTypes.LoadAuth;
}

export class AnonymousLogin implements Action {
  readonly type = AuthActionTypes.AnonymousLogin;
}

export class GoogleLogin implements Action {
  readonly type = AuthActionTypes.GoogleLogin;
}

export class AuthLoadError implements Action {
  readonly type = AuthActionTypes.AuthLoadError;
  constructor(public payload: any) {}
}

export class AuthLoaded implements Action {
  readonly type = AuthActionTypes.AuthLoaded;
  constructor(public payload: IUser) {}
}

export type AuthAction = LoadAuth | AuthLoaded | AuthLoadError;

export const fromAuthActions = {
  LoadAuth,
  AnonymousLogin,
  AuthLoaded,
  AuthLoadError
};