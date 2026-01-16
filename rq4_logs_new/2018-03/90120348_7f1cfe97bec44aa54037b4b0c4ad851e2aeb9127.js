import {
  reverseRecursive,
  reverseIterative,
  reverseBuiltin
} from './Arrays';

describe('reverse', () => {
  describe('reverseRecursive', () => {
    it('returns the reversed string', () => {
      expect(reverseRecursive('abcd')).toBe('dcba');
    });
  });

  describe('reverseIterative', () => {
    it('returns the reversed string', () => {
      expect(reverseIterative('abcd')).toBe('dcba');
    });
  });

  describe('reverseBuiltin', () => {
    it('returns the reversed string', () => {
      expect(reverseBuiltin('abcd')).toBe('dcba');
    });
  });
});