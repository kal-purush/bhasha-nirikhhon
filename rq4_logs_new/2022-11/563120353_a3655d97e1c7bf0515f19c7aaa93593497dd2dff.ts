import { AuthRequest, AuthSessionResult, makeRedirectUri, useAuthRequest as useAuthRequestFirebase } from "expo-auth-session";
import { APP_SCHEME, GIT_AUTHORIZATION_ENDPOINT, GIT_CLIENT_ID, GIT_REVOCATION_ENDPOINT, GIT_TOKEN_ENDPOINT } from "../constants";

const discovery = {
  authorizationEndpoint: GIT_AUTHORIZATION_ENDPOINT,
  tokenEndpoint: GIT_TOKEN_ENDPOINT,
  revocationEndpoint: GIT_REVOCATION_ENDPOINT,
};

export interface useAuthRequestType {
  request: AuthRequest | null, 
  response: AuthSessionResult | null, 
  promptAsync: () => Promise<AuthSessionResult>
}

export const useAuthRequest = (): useAuthRequestType => {

  const [request, response, promptAsync] = useAuthRequestFirebase(
    {
      clientId: GIT_CLIENT_ID,
      scopes: ["identity"],
      redirectUri: makeRedirectUri({
        scheme: APP_SCHEME,
      }),
    },
    discovery
  );

  return { request, response, promptAsync }
}

