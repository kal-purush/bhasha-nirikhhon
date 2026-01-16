import { expect } from 'chai';
import PhoneNumber from '../src/PhoneNumber';

describe('getNumberType', () => {
  it('returns UNKNOWN type', () => {
    const number = '+44000';
    const iso2 = 'gb';
    const numberType = PhoneNumber.getNumberType(number, iso2);
    expect(numberType).to.equal('UNKNOWN');
  });

  it('returns MOBILE type', () => {
    const number = '+447900000001';
    const iso2 = 'gb';
    const numberType = PhoneNumber.getNumberType(number, iso2);
    expect(numberType).to.equal('MOBILE');
  });

  it('returns FIXED_LINE type', () => {
    const number = '+442072212217';
    const iso2 = 'gb';
    const numberType = PhoneNumber.getNumberType(number, iso2);
    expect(numberType).to.equal('FIXED_LINE');
  });
});