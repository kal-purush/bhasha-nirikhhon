/// <reference path="../typings/node-0.10.d.ts" />
import * as CitiInterfaces from "./citiInterfaces"

var util = require('util'),
    _ = require('underscore'),
    request	= require('request'),
    cheerio = require('cheerio'),
    VError = require('verror');

export default class Citi
{
    //path: {corporate: string = '/corporatepayments/v1/'};
    path: string = '/corporatepayments/v1/';

    constructor(
        public clientId: string,
        public token?: string,
        public server?: string,
        public port?: number,
        public timeout?: number)
    {
        this.server = server || 'https://citimobilechallenge.anypresenceapp.com';
        this.port = port || 443;
        this.timeout = timeout || 20000;
    }

    private executeRequest(method: string, action: string, queryString: any, body: any, callback: (error: any, data: any) => void): void
    {
        const functionName = 'Citi.executeRequest';

        const url = this.server + ':' + this.port + this.path + action;

        const requestDesc = util.format('%s request to url %s with query string %s and body %s',
            method, url, JSON.stringify(queryString), JSON.stringify(body) );

        if (method != 'login' && !this.token) {
            const error = new VError('%s failed %s as token is null. You need to login before calling this API', functionName, requestDesc);
            return callback(error, null);
        }

        let options = {
            url: url,
            auth: {'bearer': this.token},
            method: method,
            qs: queryString,
            headers: {"User-Agent": "Citi Mobile Challenge Javascript API Client"},
            timeout: this.timeout,
            json: body };

        request(options, function(err: any, response: any, data: any)
        {
            let error: Error = null;   // default to no errors

            if(err)
            {
                error = new VError(err, '%s failed %s', functionName, requestDesc);
                error.name = err.code;
            }
            else if (response.statusCode != 200)
            {
                error = new VError(err, '%s failed %s, HTTP status code %s', functionName, requestDesc, response.statusCode);
                error.name = response.statusCode;
            }
            // if request was not able to parse json response into an object
            else if (!_.isObject(data) )
            {
                //TODO: remove this hack as request should be parsing the body of the response into a Javascript object
                // try and JSON parse it even though request should have done this
                try {
                    const json = JSON.parse(data);
                    return callback(null, json);
                }
                catch(e) {
                    error = new VError(err, '%s could not JSON parse response body from %s\nResponse body: %s', functionName, requestDesc, data);
                }

                // try and parse HTML body form response
                const $ = cheerio.load(data);

                const responseBody = $('body').text();

                if (responseBody)
                {
                    error = new VError(err, '%s could not parse response body from %s\nResponse body: %s', functionName, requestDesc, responseBody);
                    error.name = responseBody;
                }
                else
                {
                    error = new VError(err, '%s could not parse json or HTML response from %s', functionName, requestDesc);
                }
            }

            callback(error, data);
        });
    }

    login(username: string, password: string, callback: (error: any, token: string) => void): void
    {
        this.executeRequest('POST', 'login',
            // query string
            { client_id : this.clientId },
            // body
            {
                "username": username,
                "password" : password
            },
            function(err, data)
            {
                if (data && data.token) { this.token = data.token; }

                callback(err, this.token);
            });
    }

    getCorporateAccounts(callback: (error: any, accounts: [CitiInterfaces.corporateAccounts]) => void): void
    {
        this.executeRequest('GET', 'accounts', { client_id : this.clientId }, null, callback);
    }

    addCorporatePayment(callback: (error: any, addedPayment: CitiInterfaces.corporateAccountPayment) => void, newPayment: CitiInterfaces.corporateAccountPayment): void
    {
        this.executeRequest('POST', 'payments', { client_id : this.clientId }, newPayment, callback);
    }

    getCorporatePayments(callback: (error: any, payments: [CitiInterfaces.corporateAccountPayment]) => void, limit?: number, offset?: number): void
    {
        let queryString: any = { client_id : this.clientId };

        if (limit) { queryString.limit = limit; }
        if (offset) { queryString.offset = offset; }

        this.executeRequest('GET', 'payments', queryString, null, callback);
    }
}