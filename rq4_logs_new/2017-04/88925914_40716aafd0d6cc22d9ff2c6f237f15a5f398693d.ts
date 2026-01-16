import * as _ from 'lodash';
import {ThunkAction} from 'redux-thunk';

import {users, meetups} from '../mockdata';
import * as models from '../models';

const STORAGE_KEY = "AUTH_TOKEN"

function validateToken(token: string): Promise<models.User> {
    return new Promise((resolve, reject) => {
        const user = _.find(users, (u) => _.startsWith(u.email, token))
        if (user == null) {
            return reject("Where did you get this hash?")
        }
        return resolve(user);
    });
}

export interface Logout {
    type: "LOGOUT"
}
export function logout(): ThunkAction<void, models.AppState, void> {
    return (dispatch) => {
        localStorage.removeItem(STORAGE_KEY);
        return dispatch({
            type: "LOGOUT"
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
        return validateToken(token).then((user) => {
            localStorage.setItem(STORAGE_KEY, token);
            return dispatch({
                type: "LOGIN",
                token,
                user,
            });
        });
    }
}

export type AuthAction = Login | Logout

export interface AuthState {
    token: string,
    user: models.User
}

export function authReducer(state: AuthState = {
    token : localStorage.getItem(STORAGE_KEY) || "",
    user : null,
}, action: AuthAction): AuthState {
    switch (action.type) {
        case "LOGIN":
            return _.merge({}, state, {
                user: action.user,
                authToken: action.token,
            });
        case "LOGOUT":
            return _.merge({}, state, {
                user: null,
                authToken: "",
            });
        default:
            return state;
    }
}