import jwtDecode from 'jwt-decode';
import NextAuth, { NextAuthOptions } from 'next-auth';
import CredentialsProvider from 'next-auth/providers/credentials';
import GoogleProvider from 'next-auth/providers/google';
import {
  JWT,
  BackendTokensWithExpirationStamp,
} from '../../../types/userTypes';

async function refreshAccessToken(token: any) {
  const baseUrl = process.env.NEXT_PUBLIC_API_URL ?? '';
  try {
    const response = await fetch(baseUrl + 'token/refresh/', {
      method: 'POST',
      body: JSON.stringify({
        refresh: token.refresh,
      }),
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (response.ok) {
      const { access, refresh } = await response.json();
      const decoded: JWT = jwtDecode(access);
      const newToken = {
        email: decoded?.email,
        name: decoded?.name,
        user_id: decoded?.user_id,
        username: decoded?.username,
        accessTokenExpires: decoded.exp * 1000,
        access,
        refresh,
      };
      return newToken;
    }
  } catch (e: any) {
    console.error('ERROR in refreshAccessToken', e);
  }
  return token;
}

async function getUserAndTokens(
  credentials: Record<'email' | 'password', string> | undefined,
) {
  const baseUrl = process.env.NEXT_PUBLIC_API_URL ?? '';
  const response = await fetch(baseUrl + 'token/obtain/', {
    method: 'POST',
    body: JSON.stringify({
      email: credentials?.email ?? '',
      password: credentials?.password ?? '',
    }),
    headers: {
      'Content-Type': 'application/json',
    },
  });
  if (response.ok) {
    const { access, refresh } = await response.json();
    const decoded: JWT = jwtDecode(access);
    return {
      email: decoded?.email,
      name: decoded?.name,
      user_id: decoded?.user_id,
      username: decoded?.username,
      accessTokenExpires: decoded.exp * 1000,
      access,
      refresh,
    };
  }
  return null;
}

export const authOptions: NextAuthOptions = {
  secret: process.env.SECRET,
  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_ID ?? '',
      clientSecret: process.env.GOOGLE_SECRET ?? '',
    }),
    CredentialsProvider({
      name: 'Email and Password',
      credentials: {
        email: { label: 'Email', type: 'text' },
        password: { label: 'Password', type: 'password' },
      },
      // This is were you can put your own external API call to validate Email and Password
      authorize: async (credentials) => {
        try {
          const data = await getUserAndTokens(credentials);
          if (data) {
            return data as any;
          }
        } catch (e) {
          return null;
        }
      },
    }),
  ],
  callbacks: {
    async jwt({ token, account, user }) {
      if (user && account) {
        const convertedUser =
          user as unknown as BackendTokensWithExpirationStamp;
        const jwtUser = user as unknown as JWT;
        return {
          email: user.email,
          name: user.name ?? '',
          user_id: jwtUser.user_id,
          username: jwtUser.username,
          accessTokenExpires: convertedUser.accessTokenExpires,
          access: convertedUser.access,
          refresh: convertedUser.refresh,
        };
      }

      // Return previous token if the access token has not expired yet
      const isFresh =
        typeof token.accessTokenExpires === 'number' && // this is to address `of type unknown` error
        Date.now() < token.accessTokenExpires;

      if (isFresh) {
        return token;
      }
      return refreshAccessToken(token);
    },
    async session({ session, token }) {
      const access = token.access;
      const user = {
        image: '',
        email: token.email,
        name: token.name,
        username: token.username,
      };
      return { ...session, user, access: access as string };
    },
  },
  theme: {
    colorScheme: 'light',
  },
  pages: {
    signIn: '/login',
  },
};

export default NextAuth(authOptions);