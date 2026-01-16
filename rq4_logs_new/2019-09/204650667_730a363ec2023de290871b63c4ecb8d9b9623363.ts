import joi from '@hapi/joi';
import {HTTPRequestValidator} from '../src/HTTPRequestValidator';
import {createRequestMocks} from './mocks';
import {HTTPResponse} from '../src/HTTPResponse';
import {HTTPRequestValidationError} from '../src/errors/HTTPRequestValidationError';

describe('A HTTPRequestValidator', () => {
  const requestMocks = createRequestMocks();

  const httpRequestValidator = new HTTPRequestValidator({
    request:   requestMocks.request,
    joiSchema: joi.object().empty({}),
  });

  beforeEach(() => {
    requestMocks.request.execute = () => Promise.resolve(requestMocks.response);
  });

  describe('Validating a response body', () => {
    let response: HTTPResponse;

    beforeEach(async () => {
      response = await httpRequestValidator.execute();
    });

    it('Returns the http requests response', () => {
      expect(response).toEqual({
        statusCode: requestMocks.response.statusCode,
        body:       undefined,
        headers:    requestMocks.response.headers,
      });
    });
  });

  describe('Invalidating a response body', () => {
    let error: Error;

    beforeEach(async () => {
      requestMocks.request.execute = () => Promise.resolve({
        statusCode: 200,
        body:       'string',
        headers:    {},
      });

      await httpRequestValidator.execute().catch((ex) => {
        error = ex;
      });
    });

    it('Throws a HTTPRequestValidationError', () => {
      expect(error instanceof HTTPRequestValidationError).toEqual(true);
    });
  });
});