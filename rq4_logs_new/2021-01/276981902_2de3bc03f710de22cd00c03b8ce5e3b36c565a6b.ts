import { DxccRef } from './dxcc';

describe('DxccRef', () => {
  it('should get active DXCC entities by ID', () => {
    expect(DxccRef.getById(1)).toEqual({
      id: 1,
      name: 'Canada',
      continent: 'NA',
      prefixRegex: /^V[A-GOY][A-Z0-9/]*$/,
      flag: '🇨🇦',
    });
    expect(DxccRef.getById(291)).toEqual({
      id: 291,
      name: 'United States',
      continent: 'NA',
      prefixRegex: /^(K|W|N|A[A-K])[A-Z0-9/]*$/,
      flag: '🇺🇸',
    });
  });

  it('should get active DXCC entities by name', () => {
    expect(DxccRef.getByName('Canada')).toEqual({
      id: 1,
      name: 'Canada',
      continent: 'NA',
      prefixRegex: /^V[A-GOY][A-Z0-9/]*$/,
      flag: '🇨🇦',
    });
    expect(DxccRef.getByName('United States')).toEqual({
      id: 291,
      name: 'United States',
      continent: 'NA',
      prefixRegex: /^(K|W|N|A[A-K])[A-Z0-9/]*$/,
      flag: '🇺🇸',
    });
  });

  it('should find appropriate DXCC entities for simple callsigns', () => {
    expect(DxccRef.entitiesForCallsign('KE0OG')).toEqual([
      {
        id: 291,
        name: 'United States',
        continent: 'NA',
        prefixRegex: /^(K|W|N|A[A-K])[A-Z0-9/]*$/,
        flag: '🇺🇸',
      },
    ]);
    expect(DxccRef.entitiesForCallsign('LA5NTA')).toEqual([
      {
        id: 266,
        name: 'Norway',
        continent: 'EU',
        prefixRegex: /^L[A-N][A-Z0-9/]*$/,
        flag: '🇳🇴',
      },
    ]);
    expect(DxccRef.entitiesForCallsign('J72IMS')).toEqual([
      {
        id: 95,
        name: 'Dominica',
        continent: 'NA',
        prefixRegex: /^J7[A-Z0-9/]*$/,
        flag: '🇩🇲',
      },
    ]);
  });
});