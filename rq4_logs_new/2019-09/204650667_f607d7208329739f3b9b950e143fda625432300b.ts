import {HTTPResponse} from '../src/HTTPResponse';
import {HTTPRequest} from '../src/HTTPRequest';
import {HTTPRequestFactory} from '../src/HTTPRequestFactory';
import {HTTPRequestValidatorFactory} from '../src/HTTPRequestValidatorFactory';
import {HTTPRequestValidator} from '../src/HTTPRequestValidator';

export const createHTTPMocks = <T = any>() => {
  const responseMock = {
    statusCode: 200,
    body:       {},
    headers:    {},
  } as HTTPResponse<T>;
  const httpRequest  = {
    execute: () => Promise.resolve(responseMock),
  } as HTTPRequest<T>;

  return {
    factory:  {
                create: () => httpRequest,
              } as HTTPRequestFactory,
    request:  httpRequest,
    response: responseMock,
  };
};

export const createHTTPRequestValidationMocks = <T = any>() => {
  const responseMock         = {
    statusCode: 200,
    body:       {},
    headers:    {},
  } as HTTPResponse<T>;
  const httpRequestValidator = {
    execute: () => Promise.resolve(responseMock),
  } as HTTPRequestValidator<T>;

  return {
    factory:  {
                create: () => httpRequestValidator,
              } as HTTPRequestValidatorFactory,
    request:  httpRequestValidator,
    response: responseMock,
  };
};