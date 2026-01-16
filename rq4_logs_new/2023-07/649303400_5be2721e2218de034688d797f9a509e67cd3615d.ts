import { api, auth } from 'API'
import { PLATFORM } from 'API/constants'
import { handleError } from 'API/error'
import { AxiosResponse } from 'axios'
import {
  IForgotPasswordRequest,
  ILoginRequest,
  IResetPasswordRequest,
  IWebsiteLoginResponse
} from 'interfaces/authenticate'
import { IUser, IUserWithPassword } from 'interfaces/user'
import get from 'lodash/get'

export async function login(data: ILoginRequest) {
  try {
    const response = await api.post(`/auth/login`, data)
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/authenticate', 'login')
    throw new Error(errorMessage)
  }
}

export async function forgotPassword(data: IForgotPasswordRequest) {
  try {
    const response = await api.post(`/auth/forgot-password`, data)
    return response.data
  } catch (error) {
    handleError(error as Error, 'API/authenticate', 'forgotPassword')
    return {}
  }
}

export async function resetPassword(data: IResetPasswordRequest) {
  try {
    const response = await api.post(`/auth/reset-password`, data)
    return response.data
  } catch (error) {
    handleError(error as Error, 'API/authenticate', 'resetPassword')
    return {}
  }
}

export async function getUserDetail(platform: PLATFORM): Promise<IUser> {
  try {
    const response = await api.get(`/auth/profile`, {
      headers: auth(platform)
    })
    return response?.data ?? {}
  } catch (error) {
    handleError(error as Error, 'API/authenticate', 'getUserDetail')
    return {}
  }
}

export async function updateProfile(userData: IUser, platform: PLATFORM): Promise<IUser> {
  try {
    const response = await api.patch(`/auth/update-profile`, userData, {
      headers: auth(platform)
    })
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/authenticate', 'updateProfile')
    throw new Error(errorMessage)
  }
}

export async function requestOtp(phone: string): Promise<void> {
  try {
    const response = await api.put('/auth/otp/request/' + encodeURIComponent(phone))
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/authenticate', 'requestOtp')
    throw new Error(errorMessage)
  }
}

export async function verifyOtp(token: string, phone: string): Promise<boolean> {
  try {
    const response = await api.post(`/auth/otp/verify?token=${token}&phone=` + encodeURIComponent(phone))
    return response.data
  } catch (error) {
    handleError(error as Error, 'API/authenticate', 'verifyOtp')
    return false
  }
}

export async function loginWebsiteWithPhoneNumber(phoneNumber: string): Promise<IWebsiteLoginResponse> {
  try {
    const response = await api.post(`/auth/login-with-phone-number`, { phoneNumber })
    return response.data
  } catch (error) {
    handleError(error as Error, 'API/authenticate', 'loginWebsiteWithPhoneNumber')
    return {} as IWebsiteLoginResponse
  }
}

export async function loginWebsiteWithEmail(email: string, password?: string): Promise<IWebsiteLoginResponse> {
  try {
    const response = await api.post(`/auth/login-with-email`, { email, password })
    return response.data
  } catch (error) {
    handleError(error as Error, 'API/authenticate', 'loginWWebsiteWithEmail')
    return {} as IWebsiteLoginResponse
  }
}

export async function signup(userFormData: IUserWithPassword): Promise<IUser> {
  try {
    const response: AxiosResponse = await api.post(`/auth/signup`, userFormData)
    return response?.data
  } catch (error) {
    const errorMessage: string = get(error, 'message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/authenticate', 'signup')
    throw new Error(errorMessage)
  }
}

export async function verifyEmail(user: IUser): Promise<IUser> {
  try {
    const response: AxiosResponse = await api.post(`/auth/verify-email`, user)
    return response?.data
  } catch (error) {
    const errorMessage: string = get(error, 'message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/authenticate', 'verifyEmail')
    throw new Error(errorMessage)
  }
}