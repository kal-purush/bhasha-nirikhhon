// IMPORTS
// =================================================================================================
import { AuthInputs, validate } from 'nova-base';

// AUTH
// =================================================================================================
export function parseAuthHeader(header: string): AuthInputs {
    validate.authorized(header, 'Authorization header was not provided');
    const authParts = header.split(' ');
    validate.inputs(authParts.length === 2, 'Invalid authorization header');
    return {
        scheme      : authParts[0],
        credentials : authParts[1]
    };
}