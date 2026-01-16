/// <reference path="../type_declarations/DefinitelyTyped/node/node.d.ts" />
/// <reference path="../type_declarations/DefinitelyTyped/mocha/mocha.d.ts" />
import names = require('../names');
import assert = require('assert');

function testSplit(input: string, expected: string[], message?: string) {
  var actual = names.splitNames(input);
  assert.deepEqual(actual, expected, message);
}

describe('names.splitNames', () => {
  it('should split unswapped names', () => {
    testSplit('David Mimno, Hanna M Wallach, and Andrew McCallum',
       ['David Mimno', 'Hanna M Wallach', 'Andrew McCallum']);
    testSplit('Aravind K. Joshi, Ben King and Steven Abney',
      ['Aravind K. Joshi', 'Ben King', 'Steven Abney']);
    testSplit('Daniel Ramage and Chris Callison-Burch',
      ['Daniel Ramage', 'Chris Callison-Burch']);
    testSplit('David Sankofl', ['David Sankofl']);
    testSplit('Zhao et al.', ['Zhao', 'al.']);
  });

  it('should split swapped names', () => {
    // TODO: autodetect last-name-first swaps, e.g.,
    testSplit('Levy, R., & Daumé III, H.', ['R. Levy', 'H. Daumé III']);
    testSplit('Chen, S., Beeferman, Rosenfeld, R.',
      ['S. Chen', 'Beeferman', 'R. Rosenfeld']);
    testSplit('Liu, F., Tian, F., & Zhu, Q.',
      ['F. Liu', 'F. Tian', 'Q. Zhu']);
    testSplit('Valtchev, V. Kershaw, D. and Odell, J.',
      ['V. Valtchev', 'D. Kershaw', 'J. Odell']);
    // 'Liu, Tian'; is this ['Tian Liu'] or ['Liu', 'Tian']?
    testSplit('Liu, Tian', ['Tian Liu']);
  });
});