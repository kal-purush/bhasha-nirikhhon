import { encryptData, decryptData } from './utils'

export const LocalStorageEventTarget = new EventTarget()

export const setAccessTokenToLS = (accessToken: string) => {
  localStorage.setItem('accessToken', encryptData(accessToken))
}

export const setRefreshTokenToLS = (refreshToken: string) => {
  localStorage.setItem('refreshToken', encryptData(refreshToken))
}

export const clearLS = () => {
  localStorage.removeItem('accessToken')
  localStorage.removeItem('refreshToken')
  localStorage.removeItem('persist:root')

  const clearLSEvent = new Event('clearLS')
  LocalStorageEventTarget.dispatchEvent(clearLSEvent)
}

export const getAccessTokenFromLS = () => {
  if (localStorage.getItem('accessToken')) return decryptData(localStorage.getItem('accessToken'))
  return ''
}

export const getRefreshTokenFromLS = () => {
  if (localStorage.getItem('refreshToken')) return decryptData(localStorage.getItem('refreshToken'))
  return ''
}