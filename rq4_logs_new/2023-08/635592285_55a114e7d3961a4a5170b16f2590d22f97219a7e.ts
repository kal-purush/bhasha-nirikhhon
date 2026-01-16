/**
 * @link Problem definition [[docs/projecteuler/problem0013.md]]
 */

import { logger as console } from '../logger';

function problem0013alt(
  arrayOfNumbers: string[],
  numberOfFirstDigits: number
): number {
  const radix = 10;

  let sum = BigInt(0);

  arrayOfNumbers.forEach((num: string) => {
    sum += BigInt(num);
  });

  console.debug(`Sum: ${sum}`);

  const firstDigits = parseInt(
    sum.toString().slice(0, numberOfFirstDigits),
    radix
  );

  console.log(
    `First ${numberOfFirstDigits} digits of huge sume are ${firstDigits}`
  );

  return firstDigits;
}

export default problem0013alt;
export { problem0013alt };