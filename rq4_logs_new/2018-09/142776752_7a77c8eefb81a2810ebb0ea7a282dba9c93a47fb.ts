import * as util from './';

describe('util methods', () => {
  describe('classnames', () => {
    it('should work for a simple array of strings', () => {
      const res = util.classNames(['button', 'flex']);

      expect(res).toEqual('button flex');
    });

    it('should work for an array of expressions ', () => {
      let isDisabled = true;
      const res2 = util.classNames(['button', isDisabled ? 'disabled' : 'enabled']);

      expect(res2).toEqual('button disabled');
    });

    it('should filter out falsey values', () => {
      const res2 = util.classNames([
        'button',
        null,
        undefined,
        0,
        false && null && 'never',
        true && 1 && 'always'
      ]);

      expect(res2).toEqual('button always');
    });

    it('should ignore anything that isnt a string', () => {
      // @ts-ignore
      const res2 = util.classNames(['button', [true && 'never']]);

      expect(res2).toEqual('button');
    });
  });
});