import { ActionType } from 'typesafe-actions'
import { Reducer } from 'redux'
import LoginState from './types'

import * as actions from './actions'

import ActionTypes, { initialLoginState } from './constants'

const loginReducer: Reducer<LoginState, ActionType<typeof actions>> = (
    state = initialLoginState,
    action
) => {
    switch (action.type) {
        case ActionTypes.LOGIN_REQUEST:
            return {
                ...state,
                loading: true
            }
        case ActionTypes.LOGIN_SUCCESS:
            return {
                ...state,
                error: false,
                loading: false,
                loggedIn: true,
                user: action.payload
            }
        case ActionTypes.LOGIN_ERROR:
            return {
                ...state,
                loading: false,
                error: action.payload
            }
        default:
            return state
    }
}

export default loginReducer