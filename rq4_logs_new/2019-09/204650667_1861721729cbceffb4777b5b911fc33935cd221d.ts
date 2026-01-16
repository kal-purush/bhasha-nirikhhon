import {HTTPRequestDecorator} from '../src/HTTPRequestDecorator';
import {HTTPRequest} from '../src/HTTPRequest';
import {createHTTPMocks} from './mocks';
import {HTTPResponse} from '../src/HTTPResponse';

describe('A HTTPRequestDecorator', () => {
  class DecoratedHTTPRequest extends HTTPRequestDecorator<any> {
    public constructor(request: HTTPRequest<any>) {
      super(request);
    }
  }

  const requestMocks = createHTTPMocks();

  const decoratedHTTPRequest = new DecoratedHTTPRequest(requestMocks.request);

  describe('Decorating a HTTPRequest', () => {
    let response: HTTPResponse;

    beforeEach(async () => {
      response = await decoratedHTTPRequest.execute();
    });

    it('Returns the http request response', () => {
      expect(response).toBe(requestMocks.response);
    });
  });

  it('Can return the requests options', () => {
    expect(decoratedHTTPRequest.options).toBe(requestMocks.request.options);
  });
});