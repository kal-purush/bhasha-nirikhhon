/**
 * @jest-environment jsdom
 */
import 'whatwg-fetch';
import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { mockIsAcceptableCatImageTrue } from '../../mocks/api/mockIsAcceptableCatImageTrue';
import { isSuccessResult } from '../../result';
import { isAcceptableCatImage } from '../isAcceptableCatImage';

const apiUrl = 'https://api.example.com';

const mockHandlers = [
  rest.post(
    `${apiUrl}/cat-images/validation-results`,
    mockIsAcceptableCatImageTrue
  ),
];

const mockServer = setupServer(...mockHandlers);

// eslint-disable-next-line max-lines-per-function
describe('isAcceptableCatImage TestCases', () => {
  beforeAll(() => {
    mockServer.listen();
  });

  afterEach(() => {
    mockServer.resetHandlers();
  });

  afterAll(() => {
    mockServer.close();
  });

  // eslint-disable-next-line max-lines-per-function
  it('should return isAcceptableCatImage as true', async () => {
    const expected = {
      isAcceptableCatImageResponse: {
        isAcceptableCatImage: true,
      },
      xRequestId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      xLambdaRequestId: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    };

    const result = await isAcceptableCatImage({
      apiBaseUrl: apiUrl,
      accessToken: '',
      jsonRequestBody: JSON.stringify({ image: '', imageExtension: '.jpeg' }),
    });

    expect(isSuccessResult(result)).toBeTruthy();
    expect(result.value).toStrictEqual(expected);
  });
});