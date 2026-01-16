import { ContainerUserState, ContainerUserActions } from './types'

import ActionTypes from './constants'

export const initialUserState: ContainerUserState = {
    user: {}
}

function userReducer(
    state: ContainerUserState = initialUserState,
    action: ContainerUserActions
): ContainerUserState {
    switch (action.type) {
        case ActionTypes.SET_USER:
            return {
                user: action.payload
            }
        default:
            return state
    }
}

export default userReducer