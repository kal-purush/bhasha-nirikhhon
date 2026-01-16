import { splitList } from './list-utils';

describe('splitList function', () => {
  test('splits a list into parts of the specified size', () => {
    const list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const k = 3; // Split into parts of size 3

    const result = splitList(list, k);

    expect(result.parts).toHaveLength(4); // Since there are 10 elements in the list, it should be split into 4 parts
    expect(result.parts[0]).toEqual([1, 2, 3]);
    expect(result.parts[1]).toEqual([4, 5, 6]);
    expect(result.parts[2]).toEqual([7, 8, 9]);
    expect(result.parts[3]).toEqual([10]);
  });
});