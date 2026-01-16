/**
 * @jest-environment jsdom
 */
import 'whatwg-fetch';
import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { mockUploadLgtmImage } from '../../mocks/api/mockUploadLgtmImage';
import { isSuccessResult } from '../../result';
import { uploadLgtmImage } from '../uploadLgtmImage';

const apiUrl = 'https://api.example.com';

const mockHandlers = [rest.post(`${apiUrl}/lgtm-images`, mockUploadLgtmImage)];

const mockServer = setupServer(...mockHandlers);

// eslint-disable-next-line max-lines-per-function
describe('uploadLgtmImage TestCases', () => {
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
  it('should return createdLgtmImageUrl.', async () => {
    const expected = {
      createdLgtmImageUrl:
        'https://stg-lgtm-images.lgtmeow.com/2023/01/10/21/58cad97b-9732-4d71-8c50-ea2ea1f1cb51.webp',
      xRequestId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      xLambdaRequestId: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    };

    const result = await uploadLgtmImage({
      apiBaseUrl: apiUrl,
      accessToken: '',
      jsonRequestBody: JSON.stringify({ image: '', imageExtension: '.jpeg' }),
    });

    expect(isSuccessResult(result)).toBeTruthy();
    expect(result.value).toStrictEqual(expected);
  });
});