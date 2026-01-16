import { HighlightedResultPipe } from './highlighted-result.pipe';

describe('HighlightedResultPipe', () => {
  let pipe: HighlightedResultPipe;

  beforeEach(() => {
    pipe = new HighlightedResultPipe();
  });

  it('should create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should match a single keyword occurrence', () => {
    expect(pipe.transform('the cat in the hat', 'cat')).toBe('the <mark>cat</mark> in the hat');
  });

  it('should match multiple occurrences of the same keyword', () => {
    expect(pipe.transform('scams and flams', 'ams')).toBe('sc<mark>ams</mark> and fl<mark>ams</mark>');
  });

  it('should coalesce adjacent matches', () => {
    expect(pipe.transform('jeeepers', 'e')).toBe('j<mark>eee</mark>p<mark>e</mark>rs');
  });

  it('should ignore string case', () => {
  expect(pipe.transform('hAt HAT hat', 'Hat')).toBe('<mark>hAt</mark> <mark>HAT</mark> <mark>hat</mark>');
  });
});