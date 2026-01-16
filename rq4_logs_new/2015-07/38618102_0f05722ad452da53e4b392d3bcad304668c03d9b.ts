import {NetworkError} from '../network-error';

export class HttpError extends NetworkError {
    name = 'HttpError';
    statusCode: number;

    constructor(statusCode: number, message?: string) {
        super(message);
        this.statusCode = Number(statusCode) || 0;
    }

    static createFromStatusCode(statusCode: number, message?: string): HttpError {
        
        switch (statusCode) {
            case 400:
                return new HttpBadRequestError(message);
            case 401:
                return new HttpUnauthorizedError(message);
            case 403:
                return new HttpForbiddenError(message);
            case 404:
                return new HttpNotFoundError(message);
            case 405:
                return new HttpMethodNotAllowedError(message);
            case 409:
                return new HttpConflictError(message);
            case 410:
                return new HttpGoneError(message);
            case 412:
                return new HttpPreconditionFailedError(message);
            case 415:
                return new HttpUnsurpportedMediaTypeError(message);
            case 500:
                return new HttpInternalServerError(message);
            case 500:
                return new HttpNotImplementedError(message);
        }

        return new HttpError(statusCode, message);

    }
}



//-- 4xx

export class HttpBadRequestError extends HttpError {
    name = 'HttpBadRequestError';

    constructor(message?: string) {
        super(400, message);
    }
}

export class HttpUnauthorizedError extends HttpError {
    name = 'HttpUnauthorizedError';

    constructor(message?: string) {
        super(401, message);
    }
}

export class HttpForbiddenError extends HttpError {
    name = 'HttpForbiddenError';

    constructor(message?: string) {
        super(403, message);
    }
}

export class HttpNotFoundError extends HttpError {
    name = 'HttpNotFoundError';

    constructor(message?: string) {
        super(404, message);
    }
}

export class HttpMethodNotAllowedError extends HttpError {
    name = 'HttpMethodNotAllowedError';

    constructor(message?: string) {
        super(405, message);
    }
}

export class HttpConflictError extends HttpError {
    name = 'HttpConflictError';

    constructor(message?: string) {
        super(409, message);
    }
}

export class HttpGoneError extends HttpError {
    name = 'HttpGoneError';

    constructor(message?: string) {
        super(410, message);
    }
}

export class HttpPreconditionFailedError extends HttpError {
    name = 'HttpPreconditionFailedError';

    constructor(message?: string) {
        super(412, message);
    }
}

export class HttpUnsurpportedMediaTypeError extends HttpError {
    name = 'HttpUnsurpportedMediaTypeError';

    constructor(message?: string) {
        super(415, message);
    }
}



//-- 5xx

export class HttpInternalServerError extends HttpError {
    name = 'HttpInternalServerError';

    constructor(message?: string) {
        super(500, message);
    }
}

export class HttpNotImplementedError extends HttpError {
    name = 'HttpNotImplementedError';

    constructor(message?: string) {
        super(501, message);
    }
}