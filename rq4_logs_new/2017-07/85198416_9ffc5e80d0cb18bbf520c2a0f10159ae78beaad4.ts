const jwt_decode = require('jwt-decode');

export interface OnetimeToken {
    raw: string;
    data: OnetimeTokenData;
}

export interface OnetimeTokenData {
    iss: string;
    sub: string;
    exp: number;
    permissions: string[];
    roles: string[];
    url: string;
    nonce: string;
}

export function decodeOnetimeToken(token: string): OnetimeTokenData {
    try {
        return jwt_decode(token);
    } catch (e) {
        console.log("Onetime token decoding failed, token is malformed: " + token);
        return emptyOnetimeToken();
    }
}

export function emptyOnetimeToken(): OnetimeTokenData {
    return {
        iss: null,
        sub: null,
        exp: null,
        permissions: [],
        roles: [],
        url: null,
        nonce: null
    };
}