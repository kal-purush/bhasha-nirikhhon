import { sortWithNumbers } from './sort-with-numbers';

// Same tests as in BE.
describe('sortWithNumbers', () => {
  it('a is in same order', () => {
    expect.assertions(1);
    expect(['a', 'a'].sort(sortWithNumbers)).toStrictEqual(['a', 'a']);
  });

  it('a1 is before a10', () => {
    expect.assertions(1);
    expect(['a10', 'a1'].sort(sortWithNumbers)).toStrictEqual(['a1', 'a10']);
  });

  it('a_1 is before a_10', () => {
    expect.assertions(1);
    expect(['a_10', 'a_1'].sort(sortWithNumbers)).toStrictEqual(['a_1', 'a_10']);
  });

  it('vedlegg1 is before vedlegg10', () => {
    expect.assertions(1);
    expect(['vedlegg10', 'vedlegg1'].sort(sortWithNumbers)).toStrictEqual(['vedlegg1', 'vedlegg10']);
  });

  it('vedlegg 1 is before vedlegg 10', () => {
    expect.assertions(1);
    expect(['vedlegg 10', 'vedlegg 1'].sort(sortWithNumbers)).toStrictEqual(['vedlegg 1', 'vedlegg 10']);
  });

  it('abc10abc is before abc100abc', () => {
    expect.assertions(1);
    expect(['abc100abc', 'abc10abc'].sort(sortWithNumbers)).toStrictEqual(['abc10abc', 'abc100abc']);
  });

  it('1abc is before 10abc', () => {
    expect.assertions(1);
    expect(['10abc', '1abc'].sort(sortWithNumbers)).toStrictEqual(['1abc', '10abc']);
  });

  it('a is before ab', () => {
    expect.assertions(1);
    expect(['ab', 'a'].sort(sortWithNumbers)).toStrictEqual(['a', 'ab']);
  });

  it('abc123 is before abcdef', () => {
    expect.assertions(1);
    expect(['abcdef', 'abc123'].sort(sortWithNumbers)).toStrictEqual(['abc123', 'abcdef']);
  });

  it('vedlegg 2 is before vedlegg 10', () => {
    expect.assertions(1);
    expect(['vedlegg 10', 'vedlegg 2'].sort(sortWithNumbers)).toStrictEqual(['vedlegg 2', 'vedlegg 10']);
  });

  it('vedlegg 3 del 2 is before vedlegg 3 del 10', () => {
    expect.assertions(1);
    expect(['vedlegg 3 del 10', 'vedlegg 3 del 2'].sort(sortWithNumbers)).toStrictEqual([
      'vedlegg 3 del 2',
      'vedlegg 3 del 10',
    ]);
  });

  it('vedlegg 2 avsnitt 1 is before vedlegg 10 avsnitt 1', () => {
    expect.assertions(1);
    expect(['vedlegg 10 avsnitt 1', 'vedlegg 2 avsnitt 1'].sort(sortWithNumbers)).toStrictEqual([
      'vedlegg 2 avsnitt 1',
      'vedlegg 10 avsnitt 1',
    ]);
  });

  it('01-05-2014-vedlegg 2 is before 01-05-2014-vedlegg 2', () => {
    expect.assertions(1);
    expect(['01-05-2014-vedlegg 2', '01-05-2014-vedlegg 1'].sort(sortWithNumbers)).toStrictEqual([
      '01-05-2014-vedlegg 1',
      '01-05-2014-vedlegg 2',
    ]);
  });

  it('01-05-2014-vedlegg 1 avsnitt 18 is before 01-05-2014-vedlegg 2 avsnitt 16', () => {
    expect.assertions(1);
    expect(['01-05-2014-vedlegg 2 avsnitt 16', '01-05-2014-vedlegg 1 avsnitt 18'].sort(sortWithNumbers)).toStrictEqual([
      '01-05-2014-vedlegg 1 avsnitt 18',
      '01-05-2014-vedlegg 2 avsnitt 16',
    ]);
  });

  it('only first number matters', () => {
    expect.assertions(1);
    expect(
      ['Vedlegg 2 skutt', 'Vedlegg 10 arst', 'Vedlegg 3 skatt', 'Vedlegg 0 regnskap'].sort(sortWithNumbers),
    ).toStrictEqual(['Vedlegg 0 regnskap', 'Vedlegg 2 skutt', 'Vedlegg 3 skatt', 'Vedlegg 10 arst']);
  });

  it('only first number matters 2', () => {
    expect.assertions(1);
    expect(['Vedlegg2skutt', 'Vedlegg10arst', 'Vedlegg3skatt', 'Vedlegg0regnskap'].sort(sortWithNumbers)).toStrictEqual(
      ['Vedlegg0regnskap', 'Vedlegg2skutt', 'Vedlegg3skatt', 'Vedlegg10arst'],
    );
  });
});