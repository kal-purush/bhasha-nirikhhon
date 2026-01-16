import { Dispatch } from 'redux'
import { SetErrorType, SetStatusType, setStatusAC } from '../../app/app-reducer'
import { authAPI } from '../../api/todolists-api'
import { LoginDataType } from './Login'
import { handleServerAppError, handleServerNetworkError } from '../../utills/error-utills'
import { AxiosError } from 'axios'

const initialState = {
  isLoggedIn: false,
}
type InitialStateType = typeof initialState

export const authReducer = (
  state: InitialStateType = initialState,
  action: ActionsType
): InitialStateType => {
  switch (action.type) {
    case 'login/SET-IS-LOGGED-IN':
      return { ...state, isLoggedIn: action.value }
    default:
      return state
  }
}
export const setIsLoggedInAC = (value: boolean) =>
  ({ type: 'login/SET-IS-LOGGED-IN', value } as const)

export const loginTC = (data: LoginDataType) => async (dispatch: Dispatch<ActionsType>) => {
  dispatch(setStatusAC('loading'))
  try {
    const res = await authAPI.login(data)
    if (res.data.resultCode === 0) {
      dispatch(setIsLoggedInAC(true))
      dispatch(setStatusAC('succeeded'))
    } else {
      handleServerAppError(res.data, dispatch)
    }
  } catch (e) {
    handleServerNetworkError(e as { message: string }, dispatch)
  }
}

type ActionsType = ReturnType<typeof setIsLoggedInAC> | SetStatusType | SetErrorType