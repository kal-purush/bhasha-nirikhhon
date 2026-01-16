import { NextAuthOptions } from "next-auth";
import CredentialsProvider from "next-auth/providers/credentials";
import { AUTH_URL } from "@/utils/constants/urls";
import { refreshAccessToken } from "@/utils/auth";
export const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      name: "Credentials",
      credentials: {
        id: { label: "Id", type: "text" },
        email: { label: "Email", type: "text" },
        password: { label: "Password", type: "password" },
        token: { label: "Token", type: "text" },
        name: { label: "Name", type: "text" },
        role: { label: "Role", type: "text" },
        refreshToken: { label: "RefreshToken", type: "text" },
        expiresIn: { label: "ExpiresIn", type: "text" },
        userAvt: { label: "UserAvt", type: "text" },
      },
      async authorize(credentials) {
        try {
          if (credentials?.token) {
            return {
              id: credentials.id || "google-user",
              name: credentials.name,
              email: credentials.email,
              role: credentials.role || "User",
              accessToken: credentials.token,
              userAvt: credentials.userAvt,
              refreshToken: credentials.refreshToken || "",
              expiresIn: credentials.expiresIn || 3600,
            };
          }

          const res = await fetch(`${AUTH_URL}/login`, {
            method: "POST",
            body: JSON.stringify(credentials),
            headers: { "Content-Type": "application/json" },
          });

          const response = await res.json();

          if (res.ok && response.success && response.data) {
            const { user, token: accessToken, expiresIn } = response.data;

            return {
              id: user.id,
              email: user.email,
              name: user.name,
              role: user.role,
              accessToken: accessToken,
              userAvt: user.user_avt,
              refreshToken: response.data.refreshToken,
              expiresIn: response.data.expiresIn,
            };
          }

          throw new Error(response.message || "Login failed");
        } catch (error: any) {
          throw new Error(error.message || "Login failed");
        }
      },
    }),
  ],
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        token.id = user.id;
        token.email = user.email;
        token.name = user.name;
        token.role = user.role;
        token.userAvt = user.userAvt;
        token.accessToken = user.accessToken;
        token.refreshToken = user.refreshToken;
        token.accessTokenExpires = Date.now() + user.expiresIn * 1000;
      }

      const shouldRefreshTime = Math.round(
        (token.accessTokenExpires as number) - Date.now()
      );

      if (shouldRefreshTime > 5 * 60 * 1000) {
        return token;
      }

      try {
        if (!token.refreshToken) {
          throw new Error("No refresh token available");
        }
        const refreshedToken = await refreshAccessToken(token.refreshToken);

        return {
          ...token,
          accessToken: refreshedToken.accessToken,
          refreshToken: refreshedToken.refreshToken,
          accessTokenExpires: Date.now() + refreshedToken.expiresIn * 1000,
          error: undefined,
        };
      } catch (error) {
        console.error("Error refreshing token in NextAuth:", error);
        return {
          ...token,
          error: "RefreshAccessTokenError",
        };
      }
    },
    async session({ session, token }) {
      if (session.user) {
        session.user.id = token.id;
        session.user.email = token.email as string;
        session.user.name = token.name as string;
        session.user.userAvt = token.userAvt as string;
        session.user.role = token.role;
        session.user.accessToken = token.accessToken;
        session.user.refreshToken = token.refreshToken;
      }
      return session;
    },
  },
  pages: {
    signIn: "/login",
    error: "/auth/error",
  },
  session: {
    strategy: "jwt",
    maxAge: 30 * 24 * 60 * 60, // 30 days
  },
  debug: process.env.NODE_ENV === "development",
  secret: process.env.NEXTAUTH_SECRET,
};