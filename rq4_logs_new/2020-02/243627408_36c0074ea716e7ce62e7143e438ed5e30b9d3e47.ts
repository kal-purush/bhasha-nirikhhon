/// <reference types="react" />
/**
 * useFetchRefreshToken fires off a callback function on
 * mount, then executes the callback at a specified interval
 * if isAuthenticated is true. This is used to silently fetch
 * a refresh token in the background.
 *
 * More info: https://hasura.io/blog/best-practices-of-using-jwt-with-graphql/#silent_refresh
 */
declare const useFetchRefreshToken: (callback: Function, intervalRef: import("react").MutableRefObject<any>, delay: number, isAuthenticated: boolean) => void;
export default useFetchRefreshToken;