export declare enum ApiStatus {
    OK = 200,
    OpFailure = 400,
    AuthFail = 403,
    NotFound = 404,
    TooManyReqs = 429
}
export interface ApiResult {
    message: string;
    payload: any;
}
export declare function failed(message?: string, payload?: any): {
    status: ApiStatus;
    msg: string;
    payload: any;
};
export declare function notFound(message?: string, payload?: any): {
    status: ApiStatus;
    msg: string;
    payload: any;
};
export declare function ApiPermissionDenied(message?: string, payload?: any): {
    status: ApiStatus;
    msg: string;
    payload: any;
};
export declare function ApiException(status: ApiStatus, message?: string, payload?: any): {
    status: ApiStatus;
    msg: string;
    payload: any;
};