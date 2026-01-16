import { refreshToken } from "./spotify_auth";
import { getEnvVariableSafely, isTokenExpired } from "./utils";

export const CLIENT_ID = getEnvVariableSafely("CLIENT_ID");
export const redirect_uri = getEnvVariableSafely("REDIRECT_URI");
export const baseSpotifyUrl = getEnvVariableSafely("SPOTIFY_BASE_URL");

export const scopes =
  "user-read-private user-read-email user-top-read user-follow-modify";
export const params = new URLSearchParams(window.location.search);
export const code = params.get("code");

type CallbackFunction<T> = (...args: unknown[]) => T;

export async function makeAPIRequest<T>(call: CallbackFunction<T>) {
  if (isTokenExpired()) {
    // Get Refresh Token
    console.log("Token has expired, refreshing...");
    await refreshToken();
    console.log("Token refreshed.");
  }
  return await call();
}