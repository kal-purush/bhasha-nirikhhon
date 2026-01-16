import { createBoolResolver } from '../src';

describe('Resolve Bool Based', () => {

  type K = 'a' | 'b' | 'c' | 'd';

  const keys: K[] = ['a', 'b', 'c', 'd'];

  const resolve = createBoolResolver(
    keys,
  );

  test('Should resolve boolean value', () => {
    expect(resolve(true)).toEqual({
      a: true,
      b: true,
      c: true,
      d: true,
    });
    expect(resolve(false)).toEqual({
      a: false,
      b: false,
      c: false,
      d: false,
    });
  });

  test('Should resolve to undefined if not nullish', () => {
    expect(resolve('')).toBeUndefined();
    expect(resolve(0)).toBeUndefined();
    expect(resolve([])).toBeUndefined();
    expect(resolve({})).toBeUndefined();
  });

});