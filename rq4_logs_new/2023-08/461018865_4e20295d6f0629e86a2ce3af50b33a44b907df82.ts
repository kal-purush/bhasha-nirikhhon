import { describe, it, expect } from 'vitest';
import { Number } from './number.js';
import { assertType } from './testing.js';

describe('Number', () => {
  describe('.hasInstance', () => {
    it('should return true for number', () => {
      expect(Number.hasInstance(1)).toEqual(true);
    });
    it('should return false for any other value', () => {
      expect(Number.hasInstance(null)).toBe(false);
      expect(Number.hasInstance({ length: 0 })).toBe(false);
    });
    it('should refine type', () => {
      const someValue = 'true' as string | number;

      if (Number.hasInstance(someValue)) {
        assertType<typeof someValue, number>(true);
      } else {
        assertType<typeof someValue, string>(true);
      }
    });
  });
});