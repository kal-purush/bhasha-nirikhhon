import type { PayloadAction, Reducer } from '@reduxjs/toolkit'
import type { User } from '../../types/database'
import { UserActionTypes } from '../actions/userActions'

interface UserReducer {
  isLogged: boolean
  user: User | null
}

const initialState: UserReducer = {
  isLogged: false,
  user: null
}

const userReducer: Reducer<UserReducer, PayloadAction<User, keyof typeof UserActionTypes>> =
  (state = initialState, action) => {
    switch (action.type) {
    case UserActionTypes.USER_LOGIN:
      return {
        ...state,
        isLogged: true,
        user: action.payload
      }
    case UserActionTypes.USER_LOGOUT:
      return {
        ...state,
        isLogged: false,
        user: null
      }
    default:
      return state
    }
  }

export default userReducer