import { isDefined } from '../isDefined';

describe('utility function isDefined', () => {
  test('returns "false" when "undefined" argument is passed to function', () => {
    let value: undefined;
    expect(isDefined(value)).toBe(false);
    expect(isDefined(undefined)).toBe(false);
  });

  test('returns "false" when "null" argument is passed to function', () => {
    const value = null;
    expect(isDefined(value)).toBe(false);
    expect(isDefined(null)).toBe(false);
  });

  test('returns "true" for all cases where supplied argument is not "undefined" or "null"', () => {
    expect(isDefined(function () {})).toBe(true);
    expect(isDefined({})).toBe(true);
    expect(isDefined('')).toBe(true);
    expect(isDefined(0)).toBe(true);
    expect(isDefined([])).toBe(true);
  });
});