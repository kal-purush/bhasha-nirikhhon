import type { PayloadAction, Reducer } from '@reduxjs/toolkit'
import type { User } from '../../types/database'
import { AuthActionTypes } from '../actions/authActions'

interface AuthReducer {
  isLogged: boolean
  user: User | null
}

const initialState: AuthReducer = {
  isLogged: false,
  user: null
}

const userReducer: Reducer<AuthReducer, PayloadAction<User, keyof typeof AuthActionTypes>> =
  (state = initialState, action) => {
    switch (action.type) {
    case AuthActionTypes.LOGIN_AUTH: {
      return {
        isLogged: true,
        user: action.payload
      }
    }
    case AuthActionTypes.LOGOUT_AUTH: {
      return {
        isLogged: false,
        user: null
      }
    }
    default: {
      return state
    }
    }
  }

export default userReducer