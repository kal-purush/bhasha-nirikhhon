import { isNull } from '../isNull';

describe('utility function isNull', () => {
  test('returns "true" if value is null', () => {
    expect(isNull(null)).toBe(true);
  });

  test('returns "false" if value is not null', () => {
    expect(isNull('')).toBe(false);
    expect(isNull('123')).toBe(false);

    expect(isNull(0)).toBe(false);
    expect(isNull(1)).toBe(false);

    expect(isNull({})).toBe(false);
    expect(isNull({ foo: 'bar' })).toBe(false);

    expect(isNull([])).toBe(false);
    expect(isNull([1, 2])).toBe(false);

    expect(isNull(true)).toBe(false);
    expect(isNull(false)).toBe(false);

    expect(isNull(undefined)).toBe(false);
  });
});