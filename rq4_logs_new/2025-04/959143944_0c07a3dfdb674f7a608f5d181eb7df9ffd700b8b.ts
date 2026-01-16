import request from '@/utils/request'
import type {
  PasswordLoginParams,
  SmsLoginParams,
  RegisterParams,
  SmsCodeParams,
  LoginResult,
  CodeReasult
} from '@/types/auth'

export const authApi = {
  // 用户名密码登录
  loginWithPassword(params: PasswordLoginParams) {
    return request.post<LoginResult>('/login', params)
  },

  // 手机号验证码登录
  loginWithSms(params: SmsLoginParams) {
    return request.post<LoginResult>('/auth/login/sms', params)
  },

  // 发送短信验证码
  sendSmsCode(params: SmsCodeParams) {
    return request.post<CodeReasult>('/api/codeLogin', params)
  },

  // 注册
  register(params: RegisterParams) {
    return request.post<LoginResult>('/register', params)
  }
}