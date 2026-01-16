import { ActionType } from 'typesafe-actions'
import { Reducer } from 'redux'
import AuthenticationState from '../types'

import * as actions from './actions'

import ActionTypes from './constants'

import initialAuthenticationState from '../constants'

const loginReducer: Reducer<AuthenticationState, ActionType<typeof actions>> = (
    state = initialAuthenticationState,
    action
) => {
    switch (action.type) {
        case ActionTypes.LOGOUT_REQUEST:
            return {
                ...state,
                loading: true
            }
        case ActionTypes.LOGOUT_SUCCESS:
            return {
                ...state
            }
        case ActionTypes.LOGOUT_ERROR:
            return {
                ...state,
                error: action.payload
            }
        default:
            return state
    }
}

export default loginReducer