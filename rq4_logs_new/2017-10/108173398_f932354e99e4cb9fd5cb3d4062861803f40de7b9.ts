import { ActionReducerMap } from "@ngrx/store";
import { IUser } from '../../interfaces';
import * as UserActions from '../actions';

export interface State {
    users: IUser[];
}

export const reducers: ActionReducerMap<State> = {
    users: userReducer
};

export function userReducer(state = [], action) {
    switch (action.type) {
        case UserActions.LOGGED_IN:
            return [...state, Object.assign({}, action.payload)];
        case UserActions.LOGGED_OUT:
            return [];
        default:
            return state;
    }
}