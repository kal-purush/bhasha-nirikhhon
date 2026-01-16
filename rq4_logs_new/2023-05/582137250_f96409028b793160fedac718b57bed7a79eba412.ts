import { describe, it, expect, test } from 'vitest'
import BaseCalculation from '../../src/scripts/base-calc.2023';

const tests = [];

for (let income: number = 5000; income < 90000; ) {
  for (let taxgroup: number = 1; taxgroup < 6; taxgroup++) {
    const taxTests = require ('../fixture-data/base-calc_' + income + '_' + taxgroup + '.json');

    taxTests.forEach(function(test): void {
      tests.push(test);
    })
  }
  income += 5000;
}

describe('base-calc', (): void => {
  it.todo('unimplemented test');

  describe.each(tests)('calculate tax for tax class', async ({stkl, re4, lzz, krv, pvz, r, kvz, zkf, pkv, pvs, af, bk, bks, bkv, lstlzz, solzlzz}) => {

    const baseCalc = new BaseCalculation();
    await baseCalc.calculate(
      stkl,
      re4,
      lzz,
      krv,
      pvz,
      r,
      kvz,
      zkf,
      pkv,
      pvs,
      af
    );

    test(`calculate tax for tax class ${stkl}, income ${re4}, lzz: ${lzz}, krv: ${krv}`, () => {
      expect(baseCalc.getLstlzz()).toBe(lstlzz);
    })

    test(`calculate soli for tax class ${stkl}, income ${re4}, lzz: ${lzz}, krv: ${krv}`, () => {
      expect(baseCalc.getSolzlzz()).toBe(solzlzz);
    })
  });
})