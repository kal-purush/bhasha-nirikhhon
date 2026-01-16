import { createEndpoint } from '../src/util/services/fetch-iex';
import {
  PUBLIC_SERVICE,
  PRIVATE_SERVICE,
} from '../src/util/services/configurations';

const mockPath = 'stock/mock/test';
const mockParameters = { testKey: 'testValue' };

test('Succesfully create endpoint with no parameters and public config', () => {
  expect(createEndpoint(PUBLIC_SERVICE, mockPath)).toBe(
    `${PUBLIC_SERVICE.hostname}/${mockPath}?`
  );
});

test('Succesfully create endpoint with parameters and public config', () => {
  expect(createEndpoint(PUBLIC_SERVICE, mockPath, mockParameters)).toBe(
    `${PUBLIC_SERVICE.hostname}/${mockPath}?testKey=testValue`
  );
});

test('Succesfully create endpoint with no parameters and private config', () => {
  expect(createEndpoint(PRIVATE_SERVICE, mockPath)).toBe(
    `${PRIVATE_SERVICE.hostname}/${mockPath}?token=${PRIVATE_SERVICE.key}&`
  );
});

test('Succesfully create endpoint with parameters and private config', () => {
  expect(createEndpoint(PRIVATE_SERVICE, mockPath, mockParameters));
});