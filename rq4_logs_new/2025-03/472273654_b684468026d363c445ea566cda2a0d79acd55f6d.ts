import { expect } from 'vitest';

import { periodFormat } from './periodUtils';

const fom = '2020-01-02';
const tom = '2020-01-04';

describe('periodFormat', () => {
  it('should formatere ordinær periode', () => {
    expect(periodFormat(fom, tom)).toEqual('02.01.2020 - 04.01.2020');
  });

  it('should formatere periode med custom separator', () => {
    expect(periodFormat(fom, tom, { separator: 'til' })).toEqual('02.01.2020 til 04.01.2020');
  });

  it('should formatere periode med custom formatering', () => {
    expect(periodFormat(fom, tom, { year: '2-digit', month: 'long', day: 'numeric' })).toEqual(
      '2. januar 20 - 4. januar 20',
    );
  });

  it('should formatere periode med tom som er undefined', () => {
    expect(periodFormat(fom, undefined)).toEqual('02.01.2020 - ');
  });

  it('should formatere periode med tom som dagens dato', () => {
    expect(periodFormat(fom, undefined, { showTodayString: true })).toEqual('02.01.2020 - d.d.');
  });
});