import type { APIGatewayEventIdentity } from "aws-lambda"

export interface UserCognitoInfo {
    readonly userPoolId: string,
    readonly userPoolUserId: string,
    readonly authId: string,
}

const getCognitoInfo = (identity: APIGatewayEventIdentity): UserCognitoInfo => {
    const parseFailure = new Error(
        `Can't parse cognitoAuthenticationProvider value <${identity.cognitoAuthenticationProvider}>`
    );

    const [uris, cognitoSignIn, userPoolUserId] = identity.cognitoAuthenticationProvider.split(':');

    if (cognitoSignIn !== 'CognitoSignIn') {
        throw parseFailure;
    }

    if (!cognitoUserIdMatchesExpectedPattern(userPoolUserId)) {
        throw parseFailure;
    }

    const uriList = uris.split(',');

    if (uriList.length < 1) {
        throw parseFailure;
    }

    const [_server, userPoolId] = uriList[1].split('/');

    if (!userPoolId) {
        throw parseFailure;
    }

    return {
        userPoolId,
        userPoolUserId,
        authId: `cognito:${userPoolUserId}`
    };
};

const getGoogleInfo = (identity: APIGatewayEventIdentity): UserCognitoInfo => {
    const parseFailure = new Error(
        `Can't parse Google cognitoAuthenticationProvider value <${identity.cognitoAuthenticationProvider}>`
    );

    const [uris, _clientId, userId] = identity.cognitoAuthenticationProvider.split(':');

    const uriList = uris.split(',');

    if (uriList.length < 1) {
        throw parseFailure;
    }

    if (uriList[0] !== 'accounts.google.com') {
        throw parseFailure;
    }

    if (!googleUserIdMatchesExpectedPattern(userId)) {
        throw parseFailure;
    }

    return {
        userPoolId: '',
        userPoolUserId: '',
        authId: `google:${userId}`
    };
};

export const getAuthId = (identity: APIGatewayEventIdentity): UserCognitoInfo => {
    if (identity.cognitoAuthenticationProvider.startsWith('cognito-idp')) {
        return getCognitoInfo(identity);
    }

    if (identity.cognitoAuthenticationProvider.startsWith('accounts.google')) {
        return getGoogleInfo(identity);
    }

    throw new Error(`Unrecognized authentication provider: ${identity.cognitoAuthenticationProvider}`);
};

const cognitoUserIdMatchesExpectedPattern = (id: string): boolean => {
    return /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/.test(id);
};

const googleUserIdMatchesExpectedPattern = (id: string): boolean => {
    return /^\d+$/.test(id);
};