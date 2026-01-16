import { describe, it, expect } from 'vitest';
import { assign } from './assign.js';

describe('assign', () => {
  it('should return identity when null or undefined or empty object is passed', () => {
    const anyObject = { foo: true };
    expect(assign(anyObject, undefined)).toBe(anyObject);
    expect(assign(anyObject, null)).toBe(anyObject);
    expect(assign(anyObject, {})).toBe(anyObject);
  });
  it('should return a new object when another object is passed', () => {
    expect(assign({ unchanged: '', foo: true }, { foo: false })).toEqual({ unchanged: '', foo: false });
  });
  it('should return identity when values are shallow equals', () => {
    const object = { unchanged: '', foo: true };
    expect(assign(object, { foo: true })).toBe(object);
  });
  it('should not allow adding property', () => {
    const object = { unchanged: '', foo: true };
    // @ts-expect-error notAllowed is not present in object
    assign(object, { notAllowed: false });
  });
});