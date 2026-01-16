// INTERFACES
// ================================================================================================
export interface User {
    id      : string;
    token   : string;
    name    : string;
}

// DATA
// ================================================================================================
export const users = [
    {
        id      : '123',
        token   : 'token1',
        name    : 'User1'
    },
    {
        id      : '456',
        token   : 'token2',
        name    : 'User2'
    },
    {
        id      : '789',
        token   : 'token3',
        name    : 'User3'
    },
];