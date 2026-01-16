import { ActionType } from 'src/types/redux.type'
import { Token } from 'src/types/auth.type'

export const setTokenAction = (data: Token): ActionType => {
  return {
    type: 'token/SET_TOKEN',
    payload: data
  }
}

export const setAccessTokenAction = (data: { accessToken: string }): ActionType => {
  return {
    type: 'token/SET_ACCESSTOKEN',
    payload: data
  }
}

export const setRefreshTokenAction = (data: { refreshToken: string }): ActionType => {
  return {
    type: 'token/SET_REFRESHTOKEN',
    payload: data
  }
}

export const clearTokenAction = (): ActionType => {
  return {
    type: 'token/CLEAR'
  }
}