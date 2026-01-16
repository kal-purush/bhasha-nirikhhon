import { BoomError } from 'boom';
import * as Boom from 'boom';

export { BoomError };

export class HapinessError {

    private static dataInPayload(error: BoomError): BoomError {
        if (error.data && error.output && error.output.payload) {
            error.output.payload = Object.assign({ data: error.data }, error.output.payload);
        }
        return error;
    }

    static wrap(error: Error, statusCode?: number, message?: string): BoomError {
        return this.dataInPayload(Boom.wrap.apply(Boom, arguments));
    }


    static create(statusCode: number, message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.create.apply(Boom, arguments));
    }


    static badRequest(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.badRequest.apply(Boom, arguments));
    }


    static unauthorized(message?: string, scheme?: any, attributes?: any): BoomError {
        return this.dataInPayload(Boom.unauthorized.apply(Boom, arguments));
    }


    static paymentRequired(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom['paymentRequired'].apply(Boom, arguments));
    }


    static forbidden(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.forbidden.apply(Boom, arguments));
    }


    static notFound(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.notFound.apply(Boom, arguments));
    }


    static methodNotAllowed(message?: string, data?: any, allow?: string | string[]): BoomError {
        return this.dataInPayload(Boom.methodNotAllowed.apply(Boom, arguments));
    }


    static notAcceptable(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.notAcceptable.apply(Boom, arguments));
    }


    static proxyAuthRequired(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.proxyAuthRequired.apply(Boom, arguments));
    }


    static clientTimeout(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.clientTimeout.apply(Boom, arguments));
    }


    static conflict(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.conflict.apply(Boom, arguments));
    }


    static resourceGone(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.resourceGone.apply(Boom, arguments));
    }


    static lengthRequired(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.lengthRequired.apply(Boom, arguments));
    }


    static preconditionFailed(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.preconditionFailed.apply(Boom, arguments));
    }


    static entityTooLarge(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.entityTooLarge.apply(Boom, arguments));
    }


    static uriTooLong(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.uriTooLong.apply(Boom, arguments));
    }


    static unsupportedMediaType(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.unsupportedMediaType.apply(Boom, arguments));
    }


    static rangeNotSatisfiable(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.rangeNotSatisfiable.apply(Boom, arguments));
    }


    static expectationFailed(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.expectationFailed.apply(Boom, arguments));
    }


    static teapot(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.teapot.apply(Boom, arguments));
    }


    static badData(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.badData.apply(Boom, arguments));
    }


    static locked(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.locked.apply(Boom, arguments));
    }


    static preconditionRequired(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.preconditionRequired.apply(Boom, arguments));
    }


    static tooManyRequests(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.tooManyRequests.apply(Boom, arguments));
    }


    static illegal(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.illegal.apply(Boom, arguments));
    }


    static internal(message?: string, data?: any, statusCode?: number): BoomError {
        return this.dataInPayload(Boom.internal.apply(Boom, arguments));
    }

    static notImplemented(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.notImplemented.apply(Boom, arguments));
    }


    static badGateway(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.badGateway.apply(Boom, arguments));
    }


    static serverUnavailable(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.serverUnavailable.apply(Boom, arguments));
    }


    static gatewayTimeout(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.gatewayTimeout.apply(Boom, arguments));
    }


    static badImplementation(message?: string, data?: any): BoomError {
        return this.dataInPayload(Boom.badImplementation.apply(Boom, arguments));
    }
}