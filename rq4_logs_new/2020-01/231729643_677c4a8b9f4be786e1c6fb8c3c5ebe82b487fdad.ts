import { createActions, handleActions } from 'redux-actions'

const defaultState = { error: null, data: null, isLoading: false }

export const { fetchLoginStart, fetchLoginSuccess, fetchLoginFail } = createActions({
  FETCH_LOGIN_START: (email: string, pw: string) => ({ email, pw }),
  FETCH_LOGIN_SUCCESS: login => ({ login }),
  FETCH_LOGIN_FAIL: error => ({ error }),
})

export const reducer = handleActions(
  {
    [fetchLoginStart]: state => ({ ...state, error: null, isLoading: true }),
    [fetchLoginSuccess]: (state, { payload: { login } }): Promise<> => ({
      error: null,
      isLoading: false,
      data: login,
    }),
    [fetchLoginFail]: (state, { payload: { error } }) => ({ error, isLoading: false, data: [] }),
  },
  defaultState,
)