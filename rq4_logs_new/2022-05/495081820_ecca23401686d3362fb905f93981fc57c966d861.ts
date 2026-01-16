import { explode, ValueError } from '.';

describe('index.ts', () => {
  it('works', () => {
    expect(explode(',', '1,2')).toEqual(['1', '2']);
    expect(explode(' ', '1,2')).toEqual(['1,2']);
  });

  it('returns only one element when limit is 0', () => {
    expect(explode(',', '1,2', 0)).toEqual(['1,2']);
  });

  it('limits the output array', () => {
    expect(explode(',', '1,2', 1)).toEqual(['1,2']);
    expect(explode(',', '1,2', 2)).toEqual(['1', '2']);
    expect(explode(',', '1,2', 3)).toEqual(['1', '2']);
  });

  it('returns all elements except the last -limit when a negative limit is used', () => {
    expect(explode(',', '1,2,3,4', -1)).toEqual(['1', '2', '3']);
    expect(explode(',', '1,2,3,4', -2)).toEqual(['1', '2']);
    expect(explode(',', '1,2,3,4', -3)).toEqual(['1']);
    expect(explode(',', '1,2,3,4', -4)).toEqual([]);
    expect(explode(',', '1,2,3,4', -5)).toEqual([]);
  });

  it('throws a ValueError when separator is an empty string ("")', () => {
    expect(() => explode('', 'empty separator')).toThrowError(ValueError);
  });

  it('returns an empty array when separator contains a value that is not contained in string and a negative limit is used', () => {
    expect(explode(' ', '1,2,3,4', -1)).toEqual([]);
  });

  it('adds empty string ("") to the first or last element if separator appears at the start or end of string', () => {
    expect(explode(',', ',')).toEqual(['', '']);
    expect(explode(',', '1,')).toEqual(['1', '']);
    expect(explode(',', ',1')).toEqual(['', '1']);
  });
});