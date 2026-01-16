import { gql } from '@apollo/client/core';
import { apolloClient } from './appolo-client';
import { login } from './login';

const REFRESH_AUTHENTICATION = `
  mutation($request: RefreshRequest!) { 
    refresh(request: $request) {
      accessToken
      refreshToken
    }
 }
`;

const refreshAuth = (refreshToken: string) => {
    return apolloClient.mutate({
        mutation: gql(REFRESH_AUTHENTICATION),
        variables: {
            request: {
                refreshToken,
            },
        },
    });
};

export const refresh = async (address: string) => {
    console.log('refresh: address', address);

    const accessTokens = await login(address);

    const newAccessToken = await refreshAuth(
        accessTokens.authenticate.refreshToken
    );
    console.log('refresh: result', newAccessToken.data);

    return newAccessToken.data;
};