import { logger as console } from '../../logger';

import { euler001 } from './euler001';

describe('euler001', () => {
  it('euler001 Test case 0', () => {
    expect.assertions(3);

    const tests = [
      { a: 3, b: 5, n: 10, answer: 23 },
      { a: 3, b: 5, n: 100, answer: 2318 },
      { a: 3, b: 5, n: 1000, answer: 233168 }
    ];

    tests.forEach((test) => {
      const calculated = euler001(test.a, test.b, test.n);
      console.log(
        `euler001(${test.a}, ${test.b}, ${test.n}) solution found: ${test.answer}`
      );
      expect(calculated).toStrictEqual(test.answer);
    });
  });
});