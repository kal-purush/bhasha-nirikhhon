import {cutEmptyLines, isEmptyLine, trimLastEmptyLines} from '../utils';

describe('cutEmptyLines()', () => {
  it('cut whole empty lines', () => {
    const actual = cutEmptyLines([
      '.env*',
      '.envrc',
      '',
      ' ',
      '!.env.example',
      '',
    ]);
    const expected = ['.env*', '.envrc', '!.env.example'];
    expect(actual).toStrictEqual(expected);
  });
});

describe('isEmptyLine()', () => {
  it('zero-length string returns true', () => {
    expect(isEmptyLine('')).toBe(true);
  });

  it('only-whitespace string returns true', () => {
    expect(isEmptyLine(' ')).toBe(true);
  });

  it('return false', () => {
    expect(isEmptyLine('x')).toBe(false);
  });
});

describe('trimLastEmptyLines()', () => {
  it('no empty line', () => {
    const actual = trimLastEmptyLines(['a']);
    const expected = ['a'];
    expect(actual).toStrictEqual(expected);
  });
  it('no change', () => {
    const actual = trimLastEmptyLines(['a', '']);
    const expected = ['a', ''];
    expect(actual).toStrictEqual(expected);
    expect(trimLastEmptyLines(['a', ''])).toStrictEqual(['a', '']);
  });
  it('multiple empty lines', () => {
    const actual = trimLastEmptyLines(['a', '', '', '']);
    const expected = ['a', ''];
    expect(actual).toStrictEqual(expected);
    expect(trimLastEmptyLines(['a', '', '', ''])).toStrictEqual(['a', '']);
  });

  it('complexed', () => {
    const actual = trimLastEmptyLines(['a', '', 'b', '', '']);
    const expected = ['a', '', 'b', ''];
    expect(actual).toStrictEqual(expected);
  });
});