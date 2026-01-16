/**
 * @jest-environment jsdom
 */
import 'whatwg-fetch';
import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { mockInternalServerError } from '../../mocks/api/error/mockInternalServerError';
import { mockFetchLgtmImages } from '../../mocks/api/mockFetchLgtmImages';
import { mockFetchLgtmImagesUnknownResponse } from '../../mocks/api/mockFetchLgtmImagesUnknownResponse';
import { isSuccessResult } from '../../result';
import { FetchLgtmImagesInRecentlyCreated } from '../errors/FetchLgtmImagesInRecentlyCreated';
import { fetchLgtmImagesInRecentlyCreated } from '../fetchLgtmImages';

const apiUrl = 'https://api.example.com';

const mockHandlers = [
  rest.get(`${apiUrl}/lgtm-images/recently-created`, mockFetchLgtmImages),
];

const mockServer = setupServer(...mockHandlers);

// eslint-disable-next-line max-lines-per-function
describe('fetchLgtmImagesInRecentlyCreated TestCases', () => {
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
  it('should be able to fetch LGTM Images', async () => {
    const expectedLgtmImages = [
      {
        id: 1,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/00/71a7a8d4-33c2-4399-9c5b-4ea585c06580.webp',
      },
      {
        id: 2,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/00/98f86ac2-7227-44dd-bfc9-1d424b45813d.webp',
      },
      {
        id: 3,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/00/bf3bbfb8-56d3-453d-811c-0f5fd9dfa4d0.webp',
      },
      {
        id: 4,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/00/44dc9b25-a0df-4726-a2bd-fccb1e0e832e.webp',
      },
      {
        id: 5,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/00/62b7b519-9811-4e05-8c39-3c6dbab0a42d.webp',
      },
      {
        id: 6,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/01/6c7ab983-4aa1-4af4-ab37-f1327899cc26.webp',
      },
      {
        id: 7,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/01/e549cf62-c8e2-4729-af9e-b35e27bb34e3.webp',
      },
      {
        id: 8,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/01/e62cf588-057c-43a1-82a0-035d7c0e67bf.webp',
      },
      {
        id: 9,
        imageUrl:
          'https://lgtm-images.lgtmeow.com/2021/03/16/22/03b4b6a8-931c-47cf-b2e5-ff8218a67b08.webp',
      },
    ];

    const expected = {
      lgtmImages: expectedLgtmImages,
      xRequestId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      xLambdaRequestId: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    };

    const lgtmImagesResult = await fetchLgtmImagesInRecentlyCreated({
      apiBaseUrl: apiUrl,
      accessToken: '',
    });

    expect(isSuccessResult(lgtmImagesResult)).toBeTruthy();
    expect(lgtmImagesResult.value).toStrictEqual(expected);
  });

  it('should Throw FetchLgtmImagesInRecentlyCreated because the API returns an Error', async () => {
    mockServer.use(
      rest.get(
        `${apiUrl}/lgtm-images/recently-created`,
        mockInternalServerError
      )
    );

    await expect(
      fetchLgtmImagesInRecentlyCreated({ apiBaseUrl: apiUrl, accessToken: '' })
    ).rejects.toStrictEqual(
      new FetchLgtmImagesInRecentlyCreated(
        'X-Request-Id=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:X-Lambda-Request-Id:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
      )
    );
  });

  it('should return a FailureResponse because the API Response type is different', async () => {
    mockServer.use(
      rest.get(
        `${apiUrl}/lgtm-images/recently-created`,
        mockFetchLgtmImagesUnknownResponse
      )
    );

    const lgtmImagesResult = await fetchLgtmImagesInRecentlyCreated({
      apiBaseUrl: apiUrl,
      accessToken: '',
    });

    const expected = {
      invalidParams: [
        { name: 'lgtmImages', reason: 'Invalid input' },
        { name: 'lgtmImages', reason: 'Invalid url' },
        { name: 'lgtmImages', reason: 'Invalid input' },
        { name: 'lgtmImages', reason: 'Invalid url' },
      ],
      xRequestId: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
      xLambdaRequestId: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    };

    expect(isSuccessResult(lgtmImagesResult)).toBeFalsy();
    expect(lgtmImagesResult.value).toStrictEqual(expected);
  });
});