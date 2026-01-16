import envs from "../../../config/envs";

export const amplifyConfigFranchise = {
  mandatorySignin: true,
  region: envs.COGNITO.FRANCHISE.AWS_REGION,
  userPoolId: envs.COGNITO.FRANCHISE.USER_POOL_ID,
  userPoolWebClientId: envs.COGNITO.FRANCHISE.CLIENT_ID,
  authenticationFlowType: 'USER_PASSWORD_AUTH',
  Storage: 'LocalStorage',
  oauth: {
    domain: envs.COGNITO.FRANCHISE.AUTH_URL,
    scope: ['phone', 'email', 'openid', 'profile', 'aws.cognito.signin.user.admin'],
    responseType: 'token'
  }
};

export type CognitoUserModel = {
  username: string;
  password: string;
  attributes?: object;
};