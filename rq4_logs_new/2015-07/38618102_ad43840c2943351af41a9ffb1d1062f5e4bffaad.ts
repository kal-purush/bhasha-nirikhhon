/// <reference path="./_references" />

import * as http from 'putz/http';

describe('Http errors > ', function() {
	
	it('every http error type should be a HttpError', function() {
		
		function test(construc: any): boolean {
			return (new construc()) instanceof http.HttpError
		}
		
		expect(test(http.HttpBadRequestError)).toBe(true);
		expect(test(http.HttpConflictError)).toBe(true);
		expect(test(http.HttpForbiddenError)).toBe(true);
		expect(test(http.HttpGoneError)).toBe(true);
		expect(test(http.HttpInternalServerError)).toBe(true);
		expect(test(http.HttpMethodNotAllowedError)).toBe(true);
		expect(test(http.HttpNotFoundError)).toBe(true);
		expect(test(http.HttpNotImplementedError)).toBe(true);
		expect(test(http.HttpPreconditionFailedError)).toBe(true);
		expect(test(http.HttpUnauthorizedError)).toBe(true);
		expect(test(http.HttpUnsurpportedMediaTypeError)).toBe(true);
		
	});
	
	it('HttpError.createFromStatusCode should return the equivalent type', function() {
		
		function test(statusCode: number, type: any): boolean {
			return http.HttpError.createFromStatusCode(statusCode) instanceof type;
		}
		
		expect(test(400, http.HttpBadRequestError)).toBe(true);
		expect(test(401, http.HttpUnauthorizedError)).toBe(true);
		expect(test(403, http.HttpForbiddenError)).toBe(true);
		expect(test(404, http.HttpNotFoundError)).toBe(true);
		expect(test(405, http.HttpMethodNotAllowedError)).toBe(true);
		expect(test(409, http.HttpConflictError)).toBe(true);
		expect(test(410, http.HttpGoneError)).toBe(true);
		expect(test(412, http.HttpPreconditionFailedError)).toBe(true);
		expect(test(415, http.HttpUnsurpportedMediaTypeError)).toBe(true);
		expect(test(500, http.HttpInternalServerError)).toBe(true);
		expect(test(501, http.HttpNotImplementedError)).toBe(true);
		
	});
	
});