import axios from 'axios'
import { COOKIE_DOMAIN, AUTH_COOKIE_NAME, AUTH_STORE_TOKEN_KEY } from './constants'
import { persistentSession } from './index'
import { getTypedStore } from './store'

// Create axios instance with default config
export const createApiClient = () => {
  const instance = axios.create({
    timeout: 60000,
    withCredentials: true
  })

  // Request interceptor to add auth token
  instance.interceptors.request.use(async (config) => {
    try {
      // Use the persistent session to get cookies
      const cookies = await persistentSession.cookies.get({
        name: AUTH_COOKIE_NAME
      })

      // Try to get auth token (first from cookies, then from store)
      let authToken: string | null = null
      if (cookies.length > 0) {
        authToken = cookies[0].value
      } else {
        // Fallback to electron-store
        const typedStore = getTypedStore()
        authToken = typedStore.get(AUTH_STORE_TOKEN_KEY) || null
        if (authToken) {
          // Restore the cookie from the store value
          await persistentSession.cookies
            .set({
              url: `http://${COOKIE_DOMAIN}`,
              name: AUTH_COOKIE_NAME,
              value: authToken,
              domain: COOKIE_DOMAIN
            })
            .catch(() => {})
        }
      }

      if (authToken) {
        config.headers = config.headers || {}
        config.headers.Authorization = `Bearer ${authToken}`
      }

      return config
    } catch (error) {
      console.error('Error during API request:', error)
      return config
    }
  })

  return instance
}