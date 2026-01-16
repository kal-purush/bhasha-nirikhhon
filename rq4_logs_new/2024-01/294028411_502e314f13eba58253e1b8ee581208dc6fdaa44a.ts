import { fuzzySearch } from './fuzzy-search';

const TEXT = 'the quick brown fox jumps over the lazy dog';

describe('fuzzy search', () => {
  it('should work with punctuation', () => {
    expect.assertions(1);
    expect(fuzzySearch('test', 'test.')).toBeGreaterThan(0);
  });

  it('should work with numbers', () => {
    expect.assertions(1);
    expect(fuzzySearch('123', '123')).toBeGreaterThan(0);
  });

  it('should work with Norwegian characters', () => {
    expect.assertions(1);
    expect(fuzzySearch('blåbærsyltetøy', 'blåbærsyltetøy')).toBeGreaterThan(0);
  });

  it('should accept partial word hits in text', () => {
    expect.assertions(1);
    expect(fuzzySearch('test', 'this is testing')).toBeGreaterThan(0);
  });

  it('should accept partial hits in query', () => {
    expect.assertions(1);
    expect(fuzzySearch('testing', 'test')).toBeGreaterThan(0);
  });

  it('should score no hits as zero', () => {
    expect.assertions(1);
    expect(fuzzySearch('123', TEXT)).toBe(0);
  });

  it('should score longer hits better', () => {
    expect.assertions(1);

    const longer = fuzzySearch('the quick brown', TEXT);
    const shorter = fuzzySearch('the quick', TEXT);

    expect(longer).toBeGreaterThan(shorter);
  });

  it('should score hits with smaller distances between characters better', () => {
    expect.assertions(1);

    const uic = fuzzySearch('uic', TEXT);
    const qck = fuzzySearch('qck', TEXT);

    expect(uic).toBeGreaterThan(qck);
  });

  it('should score empty strings as 0', () => {
    expect.assertions(3);

    expect(fuzzySearch('', TEXT)).toBe(0);
    expect(fuzzySearch('the', '')).toBe(0);
    expect(fuzzySearch('', '')).toBe(0);
  });

  it('should score earlier occurences better than later ones', () => {
    expect.assertions(1);

    const fox = fuzzySearch('fox', TEXT);
    const dog = fuzzySearch('dog', TEXT);

    expect(fox).toBeGreaterThan(dog);
  });

  it('should score consecutive hits better than non-consecutive ones', () => {
    expect.assertions(1);

    // "test" is hidden in between the pmn characters
    const text1 = 'pmnpmnpmntpmnpnmpmnepmnpmnpnspmnpmnpnt';

    // "test" is at the end of the string
    const text2 = 'pmnmpnpmnpmnpmnpmnpmnpmnpmnpmnpmnptest';

    const worst = fuzzySearch('test', text1);
    const best = fuzzySearch('test', text2);

    expect(best).toBeGreaterThan(worst);
  });
});