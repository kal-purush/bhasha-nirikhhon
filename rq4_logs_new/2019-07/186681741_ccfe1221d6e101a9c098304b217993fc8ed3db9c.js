/* eslint-env jest */
import rules from '../server/typographyRules';

describe('Smart Quotes', () => {
  const { smartQuotes } = rules;

  const basicQuotes = 'He said, &quot;Let&#39;s party&quot;';
  const singleQuotes = 'He said, &#39;Let&#39;s party&#39;';
  const quotesAndNumbers = '&quot;I thought she was 5&#39;7&quot;&quot;';
  const nestedQuotes = '&quot;Well that&#39;s a &quot;huge&quot; relief&quot;';

  it('should replace double straight quotes with curly quotes', () => {
    const result = smartQuotes.replace(basicQuotes);
    expect(result.htmlToCopy).toBe('He said, “Let&#39;s party”');
  });

  it('should replace single straight quotes with curly quotes', () => {
    const result = smartQuotes.replace(singleQuotes);
    expect(result.htmlToCopy).toBe('He said, ‘Let&#39;s party’');
  });

  it('should ignore double quotes after a number', () => {
    const result = smartQuotes.replace(quotesAndNumbers);
    expect(result.htmlToCopy).toBe('“I thought she was 5&#39;7&quot;”');
  });

  it('should handle nested quotes', () => {
    const result = smartQuotes.replace(nestedQuotes);
    expect(result.htmlToCopy).toBe('“Well that&#39;s a “huge” relief”');
  });
});

describe('Apostrophes', () => {
  const { apostrophes } = rules;

  const simpleApostrophe = 'It&#39;s hers';
  const prime = '48&#39;';

  it('should replace single apostrophe', () => {
    const result = apostrophes.replace(simpleApostrophe);
    expect(result.htmlToCopy).toBe('It’s hers');
  });

  it('should ignore apostrophes next to digits' ,() => {
    const result = apostrophes.replace(prime);
    expect(result.htmlToCopy).toBe('48&#39;');
  });
});

describe('Dashes', () => {
  const { enDashRanges, emDashes } = rules;

  const doubleHyphen = 'Right -- that’s a better option';
  const properHyphen = 'quasi-linear';
  const range = '8-10 years';

  it('should replace double hyphens with em dash', () => {
    const result = emDashes.replace(doubleHyphen);
    expect(result.htmlToCopy).toBe('Right — that’s a better option');
  });

  it('should ignore single hyphen between words', () => {
    const result = emDashes.replace(properHyphen);
    enDashRanges.replace(result);
    expect(result.htmlToCopy).toBe('quasi-linear');
  });

  it('should replace hyphen between digits with an en dash', () => {
    const result = enDashRanges.replace(range);
    expect(result.htmlToCopy).toBe('8–10 years');
  });
});

describe('Multiplication symbols', () => {
  const { multiplicationSymbols } = rules;

  const stringWithX = 'Excellent work';
  const soleNumber = '10x';
  const twoNumbers = '3x4';
  const twoNumbersWithSpaces = '3 x 4';

  it('should ignore “x” characters in regular strings', () => {
    const result = multiplicationSymbols.replace(stringWithX);
    expect(result.htmlToCopy).toBe('Excellent work');
  });

  it('should ignore “x” character following a single number', () => {
    const result = multiplicationSymbols.replace(soleNumber);
    expect(result.htmlToCopy).toBe('10x');
  });

  it('should replace “x” character between two numbers', () => {
    const result = multiplicationSymbols.replace(twoNumbers);
    expect(result.htmlToCopy).toBe('3×4');
  });

  it('should allow for spaces around “x” symbol', () => {
    const result = multiplicationSymbols.replace(twoNumbersWithSpaces);
    expect(result.htmlToCopy).toBe('3 × 4');
  });
});

describe('Basic rule functionality', () => {
  let ruleObjects;
  beforeAll(() => {
    ruleObjects = Object.values(rules);
  });
  it('each rule should have all standard properties', () => {
    ruleObjects.forEach((rule) => {
      expect(rule).toHaveProperty('enabled');
      expect(rule).toHaveProperty('name');
      expect(rule).toHaveProperty('description');
      expect(rule).toHaveProperty('replace');
    });
  });

  it('each rule’s replace method should return an object', () => {
    const string = 'Some test copy';
    const resultObject = {
      htmlToCopy: expect.any(String),
      editorsCut: expect.any(String),
    };

    ruleObjects.forEach((rule) => {
      expect(rule.replace(string)).toMatchObject(resultObject);
    });
  });
});