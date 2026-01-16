import {createRequestMocks} from '../../mocks';
import {HTTPRequestValidationError} from '../../../src/errors/HTTPRequestValidationError';

describe('A HTTPRequestValidationError', () => {
  const requestMocks = createRequestMocks();

  const httpRequestValidationError = new HTTPRequestValidationError(requestMocks.request.options, requestMocks.response, new Error('message'));

  it('Returns correct JSON', () => {
    expect(httpRequestValidationError.toJSON()).toEqual({
      name:          httpRequestValidationError.constructor.name,
      message:       HTTPRequestValidationError.MESSAGE,
      stack:         jasmine.any(String),
      request:       requestMocks.request.options,
      response:      requestMocks.response,
      originalError: {
        name:    'Error',
        message: 'message',
        stack:   jasmine.any(String),
      },
    });
  });
});