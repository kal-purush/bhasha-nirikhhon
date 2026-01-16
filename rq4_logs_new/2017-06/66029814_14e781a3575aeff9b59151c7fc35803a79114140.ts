export declare class NotFoundError extends Error {
    constructor();
}
export declare class NotAuthorizedError extends Error {
    constructor();
}
export declare class NotAuthenticatedError extends Error {
    constructor();
}
export declare class UnknownError extends Error {
    constructor();
}
export declare type PlumpError = NotFoundError | NotAuthorizedError | NotAuthorizedError | UnknownError;