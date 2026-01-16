import { useEffect } from "react"

/**
 * useFetchRefreshToken fires off a callback function on
 * mount, then executes the callback at a specified interval
 * if isAuthenticated is true. This is used to silently fetch
 * a refresh token in the background.
 *
 * More info: https://hasura.io/blog/best-practices-of-using-jwt-with-graphql/#silent_refresh
 */

const useRefreshToken = (
  callback: Function,
  intervalRef: React.MutableRefObject<any>,
  delay: number,
  isAuthenticated: boolean,
) => {
  useEffect(() => {
    callback()
  }, [callback])

  useEffect(() => {
    if (!isAuthenticated) {
      return
    }

    intervalRef.current = setInterval(() => {
      callback()
    }, delay)

    return () => clearInterval(intervalRef.current)
  }, [callback, intervalRef, delay, isAuthenticated])
}

export default useRefreshToken