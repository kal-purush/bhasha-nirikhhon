/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable react-hooks/rules-of-hooks */
import { useMutation } from '@tanstack/react-query'
import authAPI from './auth.api'
import connect from 'src/modules/Share/constants/connect'

class ForgetPasswordCommandHandler {
  private _forgetPasswordMutation

  constructor() {
    this._forgetPasswordMutation = useMutation({
      mutationFn: (body: { userNameOrEmail: string; callBackUrl: string }) => authAPI.forgetPassword(body)
    })
  }

  handle = (data: { userNameOrEmail: string }, handleSuccess: any, handleError: any) => {
    return this._forgetPasswordMutation.mutate(
      {
        userNameOrEmail: data.userNameOrEmail,
        callBackUrl: connect.callBackUrl
      },
      {
        onSuccess: () => {
          handleSuccess()
        },
        onError: (error: any) => {
          handleError(error)
        }
      }
    )
  }

  isLoading() {
    return this._forgetPasswordMutation.isLoading
  }
}

export { ForgetPasswordCommandHandler }