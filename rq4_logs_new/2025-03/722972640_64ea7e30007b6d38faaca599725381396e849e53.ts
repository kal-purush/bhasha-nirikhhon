import convertStrapiTime from './convertStrapiTime';

describe('The convertStrapiTime util', () => {
  it('returns the correct time', () => {
    const formatNumber = jest.fn((number) =>
      number.toString().padStart(2, '0')
    );
    const time = '12:45:00.000';

    expect(convertStrapiTime(time, formatNumber)).toBe('12:45');
  });

  it('returns the correct amount of digits', () => {
    const formatNumber = jest.fn((number) =>
      number.toString().padStart(2, '0')
    );
    const time = '08:05:00.000';

    expect(convertStrapiTime(time, formatNumber)).toBe('08:05');
  });
});