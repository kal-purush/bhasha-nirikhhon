import { createEndpoint } from '../../src/util/services/fetch-iex';
import {
  PUBLIC_SERVICE,
  PRIVATE_SERVICE,
} from '../../src/util/services/configurations';

const MOCK_PATH = 'stock/mock/test';
const MOCK_PARAMETERS = { testKey: 'testValue' };

test('create endpoint with no parameters and public config', () => {
  expect(createEndpoint(PUBLIC_SERVICE, MOCK_PATH)).toBe(
    `${PUBLIC_SERVICE.hostname}/${MOCK_PATH}?`
  );
});

test('create endpoint with parameters and public config', () => {
  expect(createEndpoint(PUBLIC_SERVICE, MOCK_PATH, MOCK_PARAMETERS)).toBe(
    `${PUBLIC_SERVICE.hostname}/${MOCK_PATH}?testKey=testValue`
  );
});

test('create endpoint with no parameters and private config', () => {
  expect(createEndpoint(PRIVATE_SERVICE, MOCK_PATH)).toBe(
    `${PRIVATE_SERVICE.hostname}/${MOCK_PATH}?token=${PRIVATE_SERVICE.key}&`
  );
});

test('create endpoint with parameters and private config', () => {
  expect(createEndpoint(PRIVATE_SERVICE, MOCK_PATH, MOCK_PARAMETERS));
});