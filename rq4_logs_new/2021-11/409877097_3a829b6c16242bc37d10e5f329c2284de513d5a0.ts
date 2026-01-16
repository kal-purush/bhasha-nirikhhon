import { Auth } from 'aws-amplify';

import { CognitoUserSession } from 'amazon-cognito-identity-js';

export interface AuthSession {
  userSession: CognitoUserSession;
  accessToken?: string;
  userInfo?: User;
  userRoles?: Array<{
    id: number;
    role: string;
    userId: number;
    parkId: number;
    park: string;
  }>;
}

export const getCurrentUserToken = async (): Promise<string> => {
  const session = await Auth.currentSession();
  return session.getAccessToken().getJwtToken();
};

export const checkAuthToken = async (): Promise<string> => {
  let authToken: string;

  try {
    authToken = await getCurrentUserToken();
  } catch (error) {
    authToken = '';
  }

  return authToken;
};

export const getCurrentSession = async (): Promise<AuthSession> => {
  const session: CognitoUserSession = await Auth.currentSession();
  const accessToken: string = session.getAccessToken().getJwtToken();

  return {
    userSession: session,
    accessToken: accessToken,
  };
};