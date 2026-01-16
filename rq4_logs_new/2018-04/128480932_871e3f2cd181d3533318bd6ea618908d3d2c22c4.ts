import * as pathern from './index';

describe(`pattern`, () => {
  describe(`replace`, () => {
    it(`should return path if no matches found`, () => {
      expect(pathern.replace('a/b/c', { a: '1' })).toBe('a/b/c');
    });

    it(`should replace match with empty string if no data matches`, () => {
      expect(pathern.replace('a/:b/c', { a: '1' })).toBe('a//c');
    });

    it(`should replace match with data`, () => {
      expect(pathern.replace('postContent/:postId', { postId: '1' })).toBe('postContent/1');
    });

    it(`should replace multiple matches with data`, () => {
      expect(pathern.replace(':a/:b/:c', { a: '1', b: '2', c: '3' })).toBe('1/2/3');
    });

    it(`should replace match with string`, () => {
      expect(pathern.replace('postContent/:postId', '1')).toBe('postContent/1');
    });

    it(`should replace multiple matches with string`, () => {
      expect(pathern.replace(':a/:b/:c', '1')).toBe('1/1/1');
    });

    it(`should replace data with custom delimiter`, () => {
      expect(pathern.replace(
        'C:\\Projects\\modulo-sync\\posts\\:postId\\content.md',
        { postId: '1' },
        { delimiter: '\\' },
      )).toBe('C:\\Projects\\modulo-sync\\posts\\1\\content.md');
    });

    it(`should replace data with custom prefix`, () => {
      expect(pathern.replace(
        '$a/$b/$c',
        { a: '1', b: '2', c: '3' },
        { prefix: '$' },
      )).toBe('1/2/3');
    });
  });

  describe(`extract`, () => {
    it(`should return empty object if no matches found`, () => {
      expect(pathern.extract('a', '1')).toEqual({});
    });

    it(`should return data if match found`, () => {
      expect(pathern.extract('a/:b/c', 'a//c')).toEqual({ b: '' });
    });

    it(`should return data if multiple matches found`, () => {
      expect(pathern.extract('a/:b/c', 'a/2/c')).toEqual({ b: '2' });
    });

    it(`should ignore extra original fragments`, () => {
      expect(pathern.extract('a/:b/c/d/:e/f', 'a/2/c')).toEqual({ b: '2' });
    });

    it(`should ignore extra replaced fragments`, () => {
      expect(pathern.extract('a/:b/c', 'a/2/c/a/4/c')).toEqual({ b: '2' });
    });
  });
});