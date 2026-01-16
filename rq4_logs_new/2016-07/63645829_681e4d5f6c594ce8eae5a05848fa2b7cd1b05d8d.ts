// IMPORTS
// =================================================================================================
import { IncomingMessage, ServerResponse } from 'http';
import * as onFinished from 'on-finished';
import * as unpipe from 'unpipe';
import { Exception, InvalidEndpointError, HttpStatusCode } from 'nova-base';

// FINAL HANDLER
// =================================================================================================
export function finalhandler (request: IncomingMessage, response: ServerResponse, onerror?: (error: Error) => void) {

    return function (error: any) {

        if (!error) {
            // ignore 404 on in-flight response
            if (response.headersSent) return;

            // create invalid endpoint error
            error = new InvalidEndpointError(request['path'] || request.url);
        }

        const status = error.status || HttpStatusCode.InternalServerError;
        const headers = error.headers;
        const body = JSON.stringify((error instanceof Exception) 
            ? error 
            : { name: error.name, message: error.message }
        );        

        // schedule onerror callback
        if (onerror) {
            setImmediate(onerror, error);
        }

        if (response.headersSent) {
            // cannot actually respond
            request.socket.destroy();
        }
        else {
            // send response
            send(request, response, status, headers, body);
        }
    }
}

// HELPER FUNCTIONS
// =================================================================================================
function send(request: IncomingMessage, response: ServerResponse, status: number, headers: { [index: string]: string; }, body: string) {

    function write () {
        // response status
        response.statusCode = status;
        //res.statusMessage = statuses[status];

        // response headers
        if (headers) {
            for (let key in headers) {
                response.setHeader(key, headers[key]);
            }
        }

        // standard headers
        response.setHeader('Content-Type', 'application/json; charset=utf-8')
        //res.setHeader('Content-Length', Buffer.byteLength(body, 'utf8'))

        if (request.method === 'HEAD') {
            response.end();
        }
        else {
            response.end(body, 'utf8');
        }
    }

    if (onFinished.isFinished(request)) {
        write();
    }
    else {
        // unpipe everything from the request
        unpipe(request);

        // flush the request
        onFinished(request, write);
        request.resume();
    }
}