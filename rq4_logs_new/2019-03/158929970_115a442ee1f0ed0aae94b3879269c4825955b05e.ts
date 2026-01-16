import { Action } from '@ngrx/store';

export enum LoginActionTypes {
  Login = '[Login] Login',
  LoginSuccess = '[Login] Success Login',
  LoginFailure = '[Login] Failure Login',
  LogOff = '[Login] LogOf',
  LogOffSuccess = '[Login] Success LogOff',
  LogOffFailure = '[Login] Failure LogOff',
  GetUser = '[Login] GetUser',
  GetUserSuccess = '[Login] GetUser Success',
  GetUserFailure = '[Login] GetUser Failure',
}

export class LoginFailure implements Action {
  readonly type = LoginActionTypes.LoginFailure;
  constructor(public  error: any) {}
}
export class Login implements Action {
  readonly type = LoginActionTypes.Login;
  constructor(public login, public psw) {}
}
export class LoginSuccess implements Action {
  readonly type = LoginActionTypes.LoginSuccess;
  constructor(public token) {}
}
export class LogOffFailure implements Action {
  readonly type = LoginActionTypes.LogOffFailure;
  constructor(public  error: any) {}
}
export class LogOff implements Action {
  readonly type = LoginActionTypes.LogOff;
}
export class LogOffSuccess implements Action {
  readonly type = LoginActionTypes.LogOffSuccess;
}
export class GetUser implements Action {
  readonly type = LoginActionTypes.GetUser;
}
export class GetUserSuccess implements Action {
  readonly type = LoginActionTypes.GetUserSuccess;
  constructor(public  info: any) {}
}
export class GetUserFailure implements Action {
  readonly type = LoginActionTypes.GetUserFailure;
  constructor(public  error: any) {}
}

export type LoginActions = LoginSuccess | LoginFailure | Login
  | LogOffSuccess | LogOffFailure | LogOff | GetUser | GetUserSuccess | GetUserFailure;