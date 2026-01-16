import { Action } from '@ngrx/store';

import { AuthCredentials } from './auth.model';

export enum AuthActionTypes {
  TryToSignIn = '[Auth] Try to sign in',
  SignIn = '[Auth] Sign in',
  SignOut = '[Auth] Sign out'
}

export class TryToSignIn implements Action {
  readonly type = AuthActionTypes.TryToSignIn;

  constructor(public credentials: AuthCredentials) { }
}

export class SignIn implements Action {
  readonly type = AuthActionTypes.SignIn;

  constructor() { }
}

export class SignOut implements Action {
  readonly type = AuthActionTypes.SignOut;

  constructor() { }
}

export type AuthActions = TryToSignIn
  | SignIn
  | SignOut;