import { CombinedError, makeOperation } from '@urql/core';
import { tokenStractor, removeToken } from './store';
import { IAuth } from '../../schema';

type Payload = Pick<IAuth, 'token'>;

type GetAuthProp<T = Record<string, any>> = T & {
    authState: Payload;
};

export async function getAuth(param: any): Promise<Payload | null> {
    const { authState } = param;
    if (!authState) {
        const token = tokenStractor();
        if (token) {
            return Promise.resolve({ token });
        }
        return Promise.resolve(null);
    }

    removeToken();

    return Promise.resolve(null);
}

export function addAuthToOperation({ authState, operation }: GetAuthProp & { operation: any }): ReturnType<typeof makeOperation> {
    if (!authState || !authState.token) {
        return operation;
    }

    const fetchOptions =
        typeof operation.context.fetchOptions === 'function' ? operation.context.fetchOptions() : operation.context.fetchOptions || {};

    return makeOperation(operation.kind, operation, {
        ...operation.context,
        fetchOptions: {
            ...fetchOptions,
            headers: {
                ...fetchOptions.headers,
                Authorization: authState.token,
            },
        },
    });
}

export const didAuthError = ({ error }: { error: CombinedError }) => {
    return error.graphQLErrors.some(e => [401, 403, 409].includes(e.extensions?.code));
};

export const willAuthError = (param: any) => {
    const { authState } = param;
    if (!authState || !authState.token) return true;
    return false;
};