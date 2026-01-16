import { TypeSemver } from './semver';

describe('semver', (): void => {
  const sut = new TypeSemver();
  it('can compare equal', (): void => {
    let result = sut.compare('1.1.1', '1.1.1');
    expect(result).toBe(0);
    result = sut.compare('1.1.2', '1.1.3');
    expect(result).not.toBe(0);
  });

  it('can compare gt', (): void => {
    const result = sut.compare('2.2.2', '1.1.1');
    expect(result).toBeGreaterThan(0);
  });

  it('can compare lt', (): void => {
    const result = sut.compare('1.1.1', '3.3.3');
    expect(result).toBeLessThan(0);
  });

  it('validates valid semver', (): void => {
    const result = sut.validate('1.1.1');
    expect(result).not.toBe(null);
  });

  it('does not validate float', (): void => {
    const result = sut.validate('1.1');
    expect(result).not.toBe(null);
  });

  it('does not validate float with custom error', (): void => {
    const result = sut.validate('1.1', 'error');
    expect(result).toBe('error');
  });

  it('does not validate valid number', (): void => {
    const result = sut.validate('1');
    expect(result).not.toBe(null);
  });

  it('does not validate valid alphanum', (): void => {
    const result = sut.validate('1abc');
    expect(result).not.toBe(null);
  });

  it('does not validate valid alpha', (): void => {
    const result = sut.validate('abc');
    expect(result).not.toBe(null);
  });

  it('does not validate invalid chars', (): void => {
    const result = sut.validate('1abc.');
    expect(result).not.toBe(null);
  });

  it('can use sortFn function to sort list', (): void => {
    const list = ['1.1.1', '4.4.4', '1.1.2', '1.3.3', '2.2.2', '4.0.4'];
    list.sort(sut.compare);
    expect(list[0]).toBe('1.1.1');
    expect(list[1]).toBe('1.1.2');
    expect(list[2]).toBe('1.3.3');
    expect(list[3]).toBe('2.2.2');
    expect(list[4]).toBe('4.0.4');
    expect(list[5]).toBe('4.4.4');
  });
});