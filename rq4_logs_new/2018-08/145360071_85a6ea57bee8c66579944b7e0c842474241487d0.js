import baseApi from './base'

// 登录
const login = ({ loginName, password, rememberMe }) =>
  baseApi.request({
    method: 'post',
    url: '/login',
    data: {
      loginName,
      password,
      rememberMe
    }
  })

// 登出
const logout = ({token}) =>
  baseApi.request({
    method: 'post',
    url: '/logout',
    data: {
      token
    }
  })

// 验证token
const validateToken = token =>
  baseApi.request({
    method: 'get',
    url: '/validate',
    params: {
      token
    }
  })

// 验证密码
const validatePassword = password =>
  baseApi.request({
    method: 'post',
    url: '/validate-password',
    data: {
      password
    }
  })

export {
  login,
  logout,
  validateToken,
  validatePassword
}