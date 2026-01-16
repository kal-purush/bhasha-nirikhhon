import {ThunkAction} from 'redux-thunk';
import {push} from 'react-router-redux';

import * as auth from './services/auth';
import * as backend from './backend';
import * as models from './models';

export interface Logout {
    type: "LOGOUT"
}
export function logout(): ThunkAction<void, models.AppState, void> {
    return (dispatch) => {
        return auth.clear().then(() =>{
            return dispatch({
                type: "LOGOUT"
            });
        });
    }
}

export interface Login {
    type: "LOGIN"
    token: string,
    user: models.User,
}
export function login(token: string): ThunkAction<Promise<{}>, models.AppState, void> {
    return (dispatch) => {
        return backend.checkHash(token).then((user) => {
            return auth.set(token).then(() => {
                return user;
            })
        }).then((user) =>{
            return dispatch({
                type: "LOGIN",
                token,
                user,
            });
        }).then(() => {
            return dispatch(push("/meetups"));
        });
    }
}

export type Action = Logout | Login