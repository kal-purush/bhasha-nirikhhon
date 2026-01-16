/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable react-hooks/rules-of-hooks */
import { useMutation } from '@tanstack/react-query'
import authAPI from './auth.api'

class ForgetPasswordCommandHandler {
  private ForgetPasswordMutation

  constructor() {
    this.ForgetPasswordMutation = useMutation({
      mutationFn: (body: { userNameOrEmail: string; callBackUrl: string }) => authAPI.forgetPassword(body)
    })
  }

  handle = (data: { userNameOrEmail: string; callBackUrl: string }, handleSuccess: any, handleError: any) => {
    return this.ForgetPasswordMutation.mutate(data, {
      onSuccess: () => {
        handleSuccess()
      },
      onError: (error: any) => {
        handleError(error)
      }
    })
  }

  isLoading() {
    return this.ForgetPasswordMutation.isLoading
  }
}

export { ForgetPasswordCommandHandler }