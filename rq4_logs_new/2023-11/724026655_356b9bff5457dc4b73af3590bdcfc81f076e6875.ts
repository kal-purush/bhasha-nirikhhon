import http from 'src/utils/http'
import { LoginResponse } from 'src/types/auth.type'
import { User } from 'src/types/user.type'
import { SuccessResponse } from 'src/types/utils.type'

export const URL_SIGNUP = 'authenticate/signup'
export const URL_SIGNIN = 'authenticate/signin'
export const URL_REFRESH_TOKEN = 'authenticate/refreshtoken'

const authApi = {
  signUp: (body: Pick<User, 'email' | 'password' | 'lastName' | 'firstName' | 'phone'>) => {
    return http.post<SuccessResponse>(URL_SIGNUP, body)
  },
  signIn: (body: Pick<User, 'email' | 'password'>) => {
    return http.post<LoginResponse>(URL_SIGNIN, body)
  }
}

export default authApi