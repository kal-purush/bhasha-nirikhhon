import { isObject } from '@ch1/utility';
import { create } from './browser-dna';

describe('Browser DNA', () => {
  describe('create', () => {
    it('returns an object', () => {
      const fingerprint = create();
      expect(isObject(fingerprint)).toBe(true);
    });
  });
});