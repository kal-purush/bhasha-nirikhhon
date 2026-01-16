import {createRequestMocks} from '../../mocks';
import {HTTPRequestError} from '../../../src/errors/HTTPRequestError';

describe('A HTTPRequestError', () => {
  const requestMocks = createRequestMocks();

  const httpRequestError = new HTTPRequestError(requestMocks.request.options, new Error('message'));

  it('Returns correct JSON', () => {
    expect(httpRequestError.toJSON()).toEqual({
      name:    httpRequestError.constructor.name,
      message: HTTPRequestError.MESSAGE,
      stack:   jasmine.any(String),
      request: requestMocks.request.options,
      originalError: {
        name: 'Error',
        message: 'message',
        stack:   jasmine.any(String),
      },
    });
  });
});