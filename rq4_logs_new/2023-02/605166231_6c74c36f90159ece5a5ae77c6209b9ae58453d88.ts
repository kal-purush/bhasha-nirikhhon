import { expect, jest, test } from '@jest/globals';

const GA_MOCK_TRACKING_ID = 'YOUR_TRACKING_ID';
const GA = 'ga';
const MOCK_GOOGLE_ANALYTICS_OBJECT = 'mock-google-analytics-object';
const GOOGLE_ANALYTICS_OBJECT = 'GoogleAnalyticsObject';
const GA_TRACKING_ID = 'trackingId';
const GA_FUNCTION_NAME = 'name';

type UseCaseTypes = {
  mockPreviousCookie: boolean;
  previouslySavedCookie: string;
  hasSavedCookie: boolean;
};

const setUpMockGA = (trackingIds: string[]) => {
  return trackingIds.map((trackingId) => {
    return {
      // eslint-disable-next-line max-nested-callbacks
      get: jest.fn().mockImplementation((name) => {
        if (name === GA_TRACKING_ID) {
          return trackingId;
        } else if (name === GA_FUNCTION_NAME) {
          return 'sendFn';
        } else {
          return undefined;
        }
      }),
    };
  });
};

describe('AB testing for DataMilk Service', () => {
  beforeAll(() => {
    jest.useFakeTimers().setSystemTime(Date.UTC(2023, 1, 23));
  });

  beforeEach(() => {
    window[MOCK_GOOGLE_ANALYTICS_OBJECT] = undefined;
    window[GOOGLE_ANALYTICS_OBJECT] = undefined;
    window[GA] = undefined;
  });

  afterEach(() => {
    jest.runOnlyPendingTimers();
  });

  // eslint-disable-next-line @typescript-eslint/no-unsafe-call
  test.each([
    {
      isDirectGAObject: false,
      trackingIds: [GA_MOCK_TRACKING_ID, 'other-repeated', 'other-repeated'],
      getAllShouldThrow: false,
      gaAvailableOnAttempt: 0,
      expectingSend: true,
    },
    {
      isDirectGAObject: true,
      trackingIds: ['other-tracking-id', GA_MOCK_TRACKING_ID],
      getAllShouldThrow: false,
      gaAvailableOnAttempt: 1,
      expectingSend: true,
    },
    {
      isDirectGAObject: true,
      trackingIds: [undefined],
      getAllShouldThrow: false,
      gaAvailableOnAttempt: 0,
      expectingSend: false,
    },
    {
      isDirectGAObject: true,
      trackingIds: [undefined],
      getAllShouldThrow: true,
      gaAvailableOnAttempt: 0,
      expectingSend: false,
    },
  ])(
    'Google Analytics Availability and first event',
    ({ isDirectGAObject, trackingIds, getAllShouldThrow, gaAvailableOnAttempt, expectingSend }) => {
      jest.isolateModules(() => {
        jest.spyOn(document, 'cookie', 'get').mockReturnValue(undefined);
        jest.spyOn(Math, 'random').mockReturnValue(0.8);

        // eslint-disable-next-line max-len
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
        window.fetch = (jest.fn() as unknown as any).mockResolvedValue(null);
        const mockGetAll = jest.fn().mockImplementation(() => {
          if (getAllShouldThrow) {
            throw new Error('mock-error-on-getAll');
          }
          return setUpMockGA(trackingIds);
        });
        const mockGA = jest.fn();
        // eslint-disable-next-line dot-notation
        mockGA['getAll'] = mockGetAll;
        const configureGAMock = () => {
          if (isDirectGAObject) {
            window[GA] = mockGA;
          } else {
            window[MOCK_GOOGLE_ANALYTICS_OBJECT] = mockGA;
            window[GOOGLE_ANALYTICS_OBJECT] = MOCK_GOOGLE_ANALYTICS_OBJECT;
          }
        };
        if (gaAvailableOnAttempt === 0) {
          configureGAMock();
        }
        require('./ab_test_datamilk');
        if (gaAvailableOnAttempt === 0) {
          expect(mockGetAll).toHaveBeenCalledTimes(1);
        } else {
          expect(mockGetAll).not.toHaveBeenCalled();
        }
        if (gaAvailableOnAttempt > 0) {
          configureGAMock();
          jest.runOnlyPendingTimers();
          expect(mockGetAll).toHaveBeenCalledTimes(1);
        }
        expect(mockGA.mock.calls).toEqual(
          expectingSend
            ? [
                [
                  'sendFn.send',
                  {
                    eventAction: 'DM A/B',
                    eventCategory: 'DataMilk A/B',
                    eventLabel: 'datamilk_ab_original',
                    hitType: 'event',
                    nonInteraction: true,
                  },
                ],
              ]
            : []
        );
      });
    }
  );

  test.each([
    {
      mockPreviousCookie: false,
      previouslySavedCookie: '',
      hasSavedCookie: false,
    },
    {
      mockPreviousCookie: true,
      previouslySavedCookie: 'A',
      hasSavedCookie: true,
    },
    {
      mockPreviousCookie: true,
      previouslySavedCookie: 'something-invalid',
      hasSavedCookie: false,
    },
  ])(
    'should work for control: %s',
    async ({ mockPreviousCookie, previouslySavedCookie, hasSavedCookie }: UseCaseTypes) => {
      const performTest = () =>
        new Promise<void>((resolve) => {
          jest.isolateModules(() => {
            const spyOnGetCookie = jest
              .spyOn(document, 'cookie', 'get')
              .mockReturnValue(
                mockPreviousCookie
                  ? '_ga=GA1.2.15869483.167451; datamilk_ab_selection=' +
                      previouslySavedCookie +
                      '; _gid=GA1.2.304492020.167451; '
                  : '_ga=GA1.2.15869483.167451; _gid=GA1.2.304492020.167451; '
              );
            const spyOnSaveCookie = jest.spyOn(document, 'cookie', 'set');
            const spyOnMathRandom = jest.spyOn(Math, 'random').mockReturnValue(0.8);
            const mockFetch = jest.fn();
            const mockResponse = {
              blob: jest.fn().mockResolvedValue('mock-blob' as never),
            };
            mockFetch.mockResolvedValue(mockResponse as never);
            // eslint-disable-next-line max-len
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
            window.fetch = mockFetch as unknown as any;

            const mockGetAll = jest
              .fn()
              .mockImplementation(() => setUpMockGA([GA_MOCK_TRACKING_ID]));
            const mockSuccessOnCookie = jest.fn();
            const mockGA = jest.fn().mockImplementation(() => {
              expect(mockGA.mock.calls).toEqual([
                [
                  'sendFn.send',
                  {
                    eventAction: 'DM A/B',
                    eventCategory: 'DataMilk A/B',
                    eventLabel: 'datamilk_ab_original',
                    hitType: 'event',
                    nonInteraction: true,
                  },
                ],
              ]);
              mockGA.mock.calls.length = 0;
              expect(spyOnGetCookie).toHaveBeenCalled();
              if (hasSavedCookie) {
                expect(spyOnMathRandom).not.toHaveBeenCalled();
                expect(spyOnSaveCookie).not.toHaveBeenCalled();
              } else {
                expect(spyOnMathRandom).toHaveBeenCalled();
                expect(spyOnSaveCookie).toHaveBeenCalledWith(
                  'datamilk_ab_selection=A;expires=Thu, 20 Apr 2023 00:00:00 GMT;path=/'
                );
              }
              mockSuccessOnCookie();
              resolve();
            });
            // eslint-disable-next-line dot-notation
            mockGA['getAll'] = mockGetAll;
            window[GA] = mockGA;

            require('./ab_test_datamilk');
            expect(mockSuccessOnCookie).toHaveBeenCalled();
          });
        });
      await performTest();
    }
  );

  test.each([
    {
      mockPreviousCookie: false,
      previouslySavedCookie: '',
      hasSavedCookie: false,
    },
    {
      mockPreviousCookie: true,
      previouslySavedCookie: 'B',
      hasSavedCookie: true,
    },
    {
      mockPreviousCookie: true,
      previouslySavedCookie: 'something-invalid',
      hasSavedCookie: false,
    },
  ] as UseCaseTypes[])(
    'should work for treatment: %s',
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    async ({ mockPreviousCookie, previouslySavedCookie, hasSavedCookie }: UseCaseTypes) => {
      const performTest = () =>
        new Promise<void>((resolve) => {
          let resolveCount = 0;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          jest.isolateModules(() => {
            const spyOnGetCookie = jest
              .spyOn(document, 'cookie', 'get')
              .mockReturnValue(
                mockPreviousCookie
                  ? '_ga=GA1.2.15869483.167451; datamilk_ab_selection=' +
                      previouslySavedCookie +
                      '; _gid=GA1.2.304492020.167451; '
                  : '_ga=GA1.2.15869483.167451; _gid=GA1.2.304492020.167451; '
              );
            const spyOnSaveCookie = jest.spyOn(document, 'cookie', 'set');
            const spyOnMathRandom = jest.spyOn(Math, 'random').mockReturnValue(0.49);
            const spyOnAppendChild = jest
              .spyOn(document.head, 'appendChild')
              .mockImplementation(() => {
                return null;
              });
            let spyOnScript = null;
            const mockSuccessOnSetSrc = jest.fn();
            const mockScript = {
              // eslint-disable-next-line max-len
              // eslint-disable-next-line accessor-pairs, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-explicit-any
              set src(source: any) {
                expect(source).toEqual(
                  'https://datamilk.app/magic_ai.js?id=YOUR_DOMAIN_ID_AT_DATAMILK'
                );
                expect(spyOnGetCookie).toHaveBeenCalled();
                if (hasSavedCookie) {
                  expect(spyOnMathRandom).not.toHaveBeenCalled();
                  // expect(spyOnSaveCookie).not.toHaveBeenCalled();
                } else {
                  expect(spyOnMathRandom).toHaveBeenCalled();
                  expect(spyOnSaveCookie).toHaveBeenCalledWith(
                    'datamilk_ab_selection=B;expires=Thu, 20 Apr 2023 00:00:00 GMT;path=/'
                  );
                }
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
                expect(spyOnScript.mock.calls).toEqual([['script']]);
                mockSuccessOnSetSrc();
              },
            };
            spyOnScript = jest
              .spyOn(document, 'createElement')
              .mockReturnValue(mockScript as unknown as HTMLElement);

            const mockFetch = jest.fn();
            // eslint-disable-next-line max-len
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access
            window.fetch = mockFetch as unknown as any;
            const mockGetAll = jest
              .fn()
              .mockImplementation(() => setUpMockGA([GA_MOCK_TRACKING_ID]));
            const mockGA = jest.fn().mockImplementation(() => {
              expect(mockGA.mock.calls).toEqual([
                [
                  'sendFn.send',
                  {
                    eventAction: 'DM A/B',
                    eventCategory: 'DataMilk A/B',
                    eventLabel: 'datamilk_ab_optimized',
                    hitType: 'event',
                    nonInteraction: true,
                  },
                ],
              ]);
              if (++resolveCount === 2) {
                resolve();
              }
            });
            // eslint-disable-next-line dot-notation
            mockGA['getAll'] = mockGetAll;
            window[GA] = mockGA;

            require('./ab_test_datamilk');
            expect(spyOnAppendChild).toHaveBeenCalled();
            expect(mockSuccessOnSetSrc).toHaveBeenCalled();
            if (++resolveCount === 2) {
              resolve();
            }
          });
        });
      await performTest();
    }
  );

  it('should ignore old browsers without fetch', () => {
    jest.isolateModules(() => {
      window.fetch = undefined;
      const spyOnSetTimeout = jest.spyOn(global, 'setTimeout');
      require('./ab_test_datamilk');
      expect(spyOnSetTimeout).not.toHaveBeenCalled();
    });
  });
});

export default {};