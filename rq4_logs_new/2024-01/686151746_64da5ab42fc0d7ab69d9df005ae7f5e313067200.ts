import { AllExceptionsSocketFilter } from './general.ws.exception';

export class BadRequestExceptionWS extends AllExceptionsSocketFilter {
    constructor(
        // eslint-disable-next-line default-param-last
        type: 'asdasd',
        // error?: Error,
        error: Error
    ) {
        super(type, error);
    }
}