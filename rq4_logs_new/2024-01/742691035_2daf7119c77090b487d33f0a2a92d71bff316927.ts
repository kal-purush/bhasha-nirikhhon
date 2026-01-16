import { clsx } from './clsx';

describe('clsx', () => {
  it('return an empty string if no arguments are passed', () => {
    expect(clsx()).toBe('');
  });

  it('handle class names as strings', () => {
    expect(clsx('btn', 'btn-primary')).toBe('btn btn-primary');
  });

  it('include class names based on conditions', () => {
    expect(clsx('btn', { 'btn-primary': true, 'btn-disabled': false })).toBe(
      'btn btn-primary'
    );
  });

  it('handle a mix of strings and objects', () => {
    expect(
      clsx('btn', { 'btn-primary': true }, 'active', { hidden: false })
    ).toBe('btn btn-primary active');
  });

  it('ignore class names with false conditions', () => {
    expect(clsx('btn', { 'btn-primary': false })).toBe('btn');
  });

  it('allow multiple true conditions in a single object', () => {
    expect(clsx({ 'btn-primary': true, active: true, hidden: false })).toBe(
      'btn-primary active'
    );
  });
});