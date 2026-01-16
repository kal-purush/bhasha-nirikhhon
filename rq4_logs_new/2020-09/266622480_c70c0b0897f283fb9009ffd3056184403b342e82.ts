import { Action } from '@ngrx/store';
import { UserData } from 'src/providers/user/models';

export const AUTH_REGISTER_SUCCESS = 'AUTH_REGISTER';
export const AUTH_LOGIN_SUCCESS = 'AUTH_LOGIN';
export const AUTH_LOGOUT_SUCCESS = 'AUTH_LOGOUT';
export const AUTH_STATE_CHANGED = 'AUTH_STATE_CHANGED';

export class RegisterSuccess implements Action {
    readonly type = AUTH_REGISTER_SUCCESS;

    constructor(public payload: UserData) { }
}

export class LoginSuccess implements Action {
    readonly type = AUTH_LOGIN_SUCCESS;

    constructor(public payload: UserData) { }
}

export class LogoutSuccess implements Action {
    readonly type = AUTH_LOGOUT_SUCCESS;

    constructor() { }
}

export class AuthStateChanged implements Action {
    readonly type = AUTH_STATE_CHANGED;

    constructor(public payload: UserData) { }
}

export type AuthActions = RegisterSuccess | LoginSuccess | LogoutSuccess | AuthStateChanged;