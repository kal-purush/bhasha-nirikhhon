import { describe, expect, test } from '@jest/globals';
import { parseCityStateZip } from '../src/converters/parseCityStateZip';

describe('parseAddress', () => {
  test('should parse address with abbreviated state and numeric postal code', () => {
    const input = 'Lewiston, ID 83501';
    const expected = { city: 'Lewiston', stateOrProvince: 'ID', postalCode: '83501' };
    expect(parseCityStateZip(input)).toEqual(expected);
  });

  test('should parse address with full state name and numeric postal code', () => {
    const input = 'Seattle, Washington 98101';
    const expected = { city: 'Seattle', stateOrProvince: 'Washington', postalCode: '98101' };
    expect(parseCityStateZip(input)).toEqual(expected);
  });

  test('should parse address with international postal code', () => {
    const input = 'Toronto, Ontario M5H 2N2';
    const expected = { city: 'Toronto', stateOrProvince: 'Ontario', postalCode: 'M5H 2N2' };
    expect(parseCityStateZip(input)).toEqual(expected);
  });

  test('should parse address with international postal code', () => {
    const input = 'Vancouver, British Columbia V6B 1H5';
    const expected = { city: 'Vancouver', stateOrProvince: 'British Columbia', postalCode: 'V6B 1H5' };
    expect(parseCityStateZip(input)).toEqual(expected);
  });

  test('should parse address with multi-word city and abbreviated state', () => {
    const input = 'Salt Lake City, UT 84101';
    const expected = { city: 'Salt Lake City', stateOrProvince: 'UT', postalCode: '84101' };
    expect(parseCityStateZip(input)).toEqual(expected);
  });

  test('should handle invalid address format', () => {
    const input = 'Invalid Address';
    expect(parseCityStateZip(input)).toBeNull();
  });

  test('should handle addresses with extra spaces', () => {
    const input = '   Los Angeles  ,   California   90001   ';
    const expected = { city: 'Los Angeles', stateOrProvince: 'California', postalCode: '90001' };
    expect(parseCityStateZip(input)).toEqual(expected);
  });
});