import isSameDate from './isSameDate';

describe('The getCountryFlag util', () => {
  it('returns flag icon', () => {
    expect(
      isSameDate(
        new Date('2024-02-12T08:30:00Z'),
        new Date('2024-02-12T08:30:00Z')
      )
    ).toEqual(true);
    expect(
      isSameDate(
        new Date('2025-02-12T08:30:00Z'),
        new Date('2024-02-10T08:30:00Z')
      )
    ).toEqual(false);
  });
});