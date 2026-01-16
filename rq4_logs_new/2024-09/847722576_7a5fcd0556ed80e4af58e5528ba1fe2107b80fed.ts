import { NextAuthOptions } from "next-auth";
import CredentailsProvider from "next-auth/providers/credentials";

const getParams = async () => {
  const resp = await fetch("http://localhost:5000/api/login", {
    method: "GET",
  });

  const { url } = await resp.json();
  const params = new URLSearchParams(url.split("?")[1]);

  const client_id = params.get("client_id");
  const response_type = params.get("response_type");
  const scope = params.get("scope");
  const state = params.get("state");

  return {
    client_id,
    response_type,
    scope,
    state,
  };
};

export const createAuthOptions = async (params) => {
  const params = await getParams();
  const authOptions: NextAuthOptions = {
    providers: [
      {
        id: "square",
        name: "Square",
        type: "oauth",
        authorization: {
          url: "https://connect.squareupsandbox.com/oauth2/authorize",
          params: {
            client_id: params.client_id ?? "",
            response_type: params.response_type ?? "",
            scope: params.scope ?? "",
            state: params.state ?? "",
          },
        },
        token: "http://localhost:5000/api/access-token?code=",
        userinfo: "https://kapi.kakao.com/v2/user/me",
        profile(profile) {
          return {
            id: profile.id,
            name: profile.kakao_account?.profile.nickname,
            email: profile.kakao_account?.email,
            image: profile.kakao_account?.profile.profile_image_url,
          };
        },
      },
    ],
    session: {
      strategy: "jwt",
    },
    callbacks: {
      async jwt({ token, account }) {
        if (account?.access_token) {
          token.accessToken = account.access_token;
        }
        return token;
      },
      async session({ session, token }) {
        // Type declaration for session extended in types/next-auth.d.ts to include accessToken
        session.accessToken = token.accessToken as string;
        return session;
      },
    },
    secret: process.env.NEXTAUTH_SECRET,
  };
  return authOptions;
};