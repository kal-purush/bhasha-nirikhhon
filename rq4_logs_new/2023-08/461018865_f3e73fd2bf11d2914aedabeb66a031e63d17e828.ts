import { describe, expect, it } from 'vitest';
import { taskStub } from './task.js';
import { unsafeRun } from '../run.js';
import { Result } from '../result.js';

describe('taskStub', () => {
  it('should resolve value', () => {
    const task = taskStub({ value: 'anyValue' });
    const result = unsafeRun(task);
    expect(result).toEqual(Result.Ok('anyValue'));
  });
  it('should reject value', () => {
    const task = taskStub({ error: 'anyError' });
    const result = unsafeRun(task);
    expect(result).toEqual(Result.Error('anyError'));
  });
  it('should throw error', () => {
    const task = taskStub({ throwError: 'MockError' });
    expect(() => unsafeRun(task)).toThrow('MockError');
  });
});