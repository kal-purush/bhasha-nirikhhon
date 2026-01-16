import { signIn } from "next-auth/react";
import { AUTH_URL } from "@/utils/constants/urls";

export async function refreshAccessToken(refreshToken: string) {
  try {
    const response = await fetch(`${AUTH_URL}/refresh-token`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        refreshToken: refreshToken,
      }),
    });

    const data = await response.json();

    if (response.status === 401) {
      throw new Error("Refresh token has expired");
    }

    if (!response.ok) {
      throw new Error(data.message || "Failed to refresh token");
    }

    return {
      accessToken: data.data.token,
      refreshToken: data.data.refreshToken,
      expiresIn: data.data.expiresIn,
    };
  } catch (error) {
    console.error("Error refreshing token:", error);
    throw error;
  }
}

export async function refreshTokenAndSignIn(refreshToken: string) {
  try {
    const refreshResult = await refreshAccessToken(refreshToken);

    // Đăng nhập lại với token mới
    const result = await signIn("credentials", {
      redirect: false,
      token: refreshResult.accessToken,
      refreshToken: refreshResult.refreshToken,
      expiresIn: refreshResult.expiresIn,
    });

    if (result?.error) {
      throw new Error("Failed to sign in with new token");
    }

    return refreshResult;
  } catch (error) {
    console.error("Error refreshing token and signing in:", error);
    throw error;
  }
}