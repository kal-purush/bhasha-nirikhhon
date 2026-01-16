import { useCallback, useState } from 'react'

export const useReadCookie = <T>(key: string): [T | undefined, () => void] => {
  const getCookie = useCallback((): T | undefined => {
    try {
      const cookies: { [key: string]: string } = {}
      document.cookie.split(';').forEach(cookie => {
        const splittedCookie = cookie.split('=')
        cookies[splittedCookie[0]] = splittedCookie[1]
      })

      const cookieValue = JSON.parse(cookies[key]) as T

      return cookieValue
    } catch (error) {
      console.warn('')
      return undefined
    }
  }, [key])

  const [value, setValue] = useState<T | undefined>(getCookie)

  const reload = useCallback(() => {
    const cookieValue = getCookie()
    setValue(cookieValue)
  }, [getCookie])

  return [value, reload]
}