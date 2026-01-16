import timezoneMock from 'timezone-mock';

import { makeDateFormatter } from './date-formater';

describe('makeDateFormatter factory function', () => {
  beforeAll(() => {
    timezoneMock.register('Etc/GMT-3');
  });

  afterAll(() => {
    timezoneMock.unregister();
  });

  it('creates a date formatter', () => {
    const format = makeDateFormatter('en');

    expect(format).toBeInstanceOf(Function);
  });

  it('formats date in portuguese', () => {
    const format = makeDateFormatter('pt');
    const date = format(new Date(2025, 0, 1), 'PPP');

    expect(date).toBe('1 de janeiro de 2025');
  });

  it('formats date in english', () => {
    const format = makeDateFormatter('en');
    const date = format(new Date(2025, 0, 1), 'PPP');

    expect(date).toBe('January 1st, 2025');
  });

  it('formats date in spanish', () => {
    const format = makeDateFormatter('es');
    const date = format(new Date(2025, 0, 1), 'PPP');

    expect(date).toBe('1 de enero de 2025');
  });

  it('formats date in german', () => {
    const format = makeDateFormatter('de');
    const date = format(new Date(2025, 0, 1), 'PPP');

    expect(date).toBe('1. Januar 2025');
  });

  it('accepts a timestamp as a date', () => {
    const format = makeDateFormatter('en');
    const numericDate = new Date(2025, 0, 1).getTime();

    const date = format(numericDate, 'PPP');
    expect(date).toBe('January 1st, 2025');
  });

  it('accepts a string in ISO format', () => {
    const format = makeDateFormatter('en');
    const isoDate = '2025-01-01T00:00:00Z';

    const date = format(isoDate, 'PPP');
    expect(date).toBe('January 1st, 2025');
  });

  it('accepts a string in yyyy-MM-dd format', () => {
    const format = makeDateFormatter('en');
    const dateStr = '2025-01-01';

    const date = format(dateStr, 'PPP');
    expect(date).toBe('January 1st, 2025');
  });
});