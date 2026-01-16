// IMPORTS
// =================================================================================================
import * as url from 'url';
import * as qs from 'querystring';
import { RequestHandler, Request, Response } from 'router';
import * as proxyaddr from 'proxy-addr';
import * as onHeaders from 'on-headers';
import { util } from 'nova-base';
import { WebServerConfig } from './../Application';
import { compileTrust } from './../util';

// MODULE VARIABLES
// =================================================================================================
const headers = {
    SERVER_NAME     : 'X-Server-Name',
    API_VERSION     : 'X-Api-Version',
    RESPONSE_TIME   : 'X-Response-Time'
};

// FIRST HANDLER
// =================================================================================================
export function firsthandler(name: string, version: string, options: WebServerConfig) {

    // set up trust function for proxy address
    const trustFunction = compileTrust(options.trustProxy);

    return function (request: Request, response: Response, next: (error?: Error) => void) {

        const start = process.hrtime();

        // set basic headers
        onHeaders(response, function(this: Response) {
            this.setHeader(headers.SERVER_NAME, name);
            this.setHeader(headers.API_VERSION, version);
            this.setHeader(headers.RESPONSE_TIME, Math.round(util.since(start)).toString());
        });

        // parse URL
        if (request._parsedUrl) {
            request.path = request._parsedUrl.pathname;
            request.query = qs.parse(request._parsedUrl.query);
        }
        else {
            const parsedUrl = url.parse(request.url, true);
            request.path = parsedUrl.pathname;
            request.query = parsedUrl.query;
        }

        // get IP address of the request
        request.ip = proxyaddr(request, trustFunction);

        // continue to the next handler
        next();
    }
}