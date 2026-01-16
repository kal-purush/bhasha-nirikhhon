import getAvailableLocalesFromStrapi from './getAvailableLocalesFromStrapi';
import strapiClient from './strapiClient';

jest.mock('./strapiClient', () => ({
  get: jest.fn(),
}));

describe('getAvailableLocales function', () => {
  it('should fetch available locales and return them correctly', async () => {
    const mockResponse = {
      data: [{ code: 'en' }, { code: 'de' }, { code: 'hu' }],
    };

    (strapiClient.get as jest.Mock).mockResolvedValue(mockResponse);

    const locales = await getAvailableLocalesFromStrapi();

    expect(locales).toEqual(['en', 'de', 'hu']);
    expect(strapiClient.get).toHaveBeenCalledWith('/i18n/locales');
    expect(strapiClient.get).toHaveBeenCalledTimes(1);
  });
});