import {createRequestMocks} from '../../mocks';
import {HTTPRequestInvalidStatusCodeError} from '../../../src/errors/HTTPRequestInvalidStatusCodeError';

describe('A HTTPRequestValidationError', () => {
  const requestMocks = createRequestMocks();

  const httpRequestInvalidStatusCodeError = new HTTPRequestInvalidStatusCodeError(requestMocks.request.options, requestMocks.response);

  it('Returns correct JSON', () => {
    expect(httpRequestInvalidStatusCodeError.toJSON()).toEqual({
      name:          httpRequestInvalidStatusCodeError.constructor.name,
      message:       HTTPRequestInvalidStatusCodeError.MESSAGE,
      stack:         jasmine.any(String),
      request:       requestMocks.request.options,
      response:      requestMocks.response,
    });
  });
});