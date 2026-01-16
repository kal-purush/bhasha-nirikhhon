import joi from '@hapi/joi';
import {HTTPRequestValidatorFactory} from '../../src/HTTPRequestValidatorFactory';
import {createRequestMocks} from '../mocks';
import {HTTPRequestValidator} from '../../src/HTTPRequestValidator';

describe('A HTTPRequestValidatorFactory', () => {
  const requestMocks = createRequestMocks();

  const httpRequestValidatorFactory = new HTTPRequestValidatorFactory();

  describe('Creating a HTTPRequestValidator', () => {
    const request = httpRequestValidatorFactory.create(requestMocks.request, joi.object());

    it('Is a HTTPRequestValidator', () => {
      expect(request instanceof HTTPRequestValidator).toEqual(true);
    });
  });
});