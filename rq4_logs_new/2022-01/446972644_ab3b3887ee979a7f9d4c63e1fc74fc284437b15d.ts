import { Foo } from './Foo';

describe('Foo', () => {
  describe('bar', () => {
    it('should return true', () => {
      expect(Foo.bar()).toBeTruthy();
    });
  });
});