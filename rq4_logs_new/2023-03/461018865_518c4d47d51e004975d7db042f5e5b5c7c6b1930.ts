import { describe, expect, it, vi } from 'vitest';
import { throwError } from './prelude.js';
import { Result } from './result.js';
import { Canceler, unsafeRun, unsafeRunOk } from './run.js';
import { Task } from './task.js';

const anyError = new Error('TestError');
const anyObject = Object.freeze({ foo: true });
const waitMs = (ms: number) =>
  ms === 0
    ? Promise.resolve()
    : new Promise<void>((resolve) => {
        setTimeout(() => resolve(), ms);
      });
const generateTask = <V = never, E = never>(
  options: {
    canceler?: () => void;
    delayMs?: number;
  } & ({ value: V } | { error: E } | { throwError: unknown })
) => {
  const { canceler = () => {}, delayMs } = options;
  const isAsync = delayMs != null && delayMs >= 0;

  return isAsync === true
    ? Task<V, E>(async ({ ok, error, setCanceler }) => {
        setCanceler(canceler);
        await waitMs(delayMs ?? 0);
        return 'value' in options
          ? ok(options.value)
          : 'error' in options
          ? error(options.error)
          : throwError(options.throwError);
      })
    : Task<V, E>(({ ok, error }) =>
        'value' in options
          ? ok(options.value)
          : 'error' in options
          ? error(options.error)
          : throwError(options.throwError)
      );
};

describe('Canceler', () => {
  describe('()', () => {
    it('should return a new Canceler', () => {
      expect(Canceler()).toEqual({ current: undefined });
    });
    it('should initialize using initialValue', () => {
      const fn = vi.fn();
      expect(Canceler(fn)).toEqual({ current: fn });
    });
  });
  describe('.clear', () => {
    it('', () => {
      const canceler = Canceler(() => {});
      Canceler.clear(canceler);
      expect(canceler).toEqual({ current: undefined });
    });
  });
  describe('.cancel', () => {
    it('should run the canceler function', () => {
      const fn = vi.fn();
      const canceler = Canceler(fn);
      Canceler.cancel(canceler);
      expect(fn).toHaveBeenCalledOnce();
    });
  });
});

describe('unsafeRun', () => {
  it('should run throwing task', () => {
    const task = Task(() => {
      throw anyError;
    });
    expect(() => unsafeRun(task)).toThrow(anyError);
  });
  it('should return the result of task.taskRun() for sync', () => {
    expect(unsafeRun(Task(({ ok }) => ok(anyObject)))).toEqual(Result.Ok(anyObject));
  });
  it('should return the result of task.taskRun() for async', async () => {
    await expect(
      unsafeRun(
        Task(
          ({ ok }) =>
            new Promise<Result<typeof anyObject, never>>((resolve) => {
              setTimeout(() => resolve(ok(anyObject)), 0);
            })
        )
      )
    ).resolves.toEqual(Result.Ok(anyObject));
  });
  it('should run rejected task', async () => {
    const task = Task(() => Promise.reject(new Error('TestError')));
    await expect(unsafeRun(task)).rejects.toEqual(new Error('TestError'));
  });
});
describe('unsafeRunOk', () => {
  it('should run throwing task', () => {
    const task = generateTask({ throwError: anyError });

    expect(() => unsafeRunOk(task)).toThrow(anyError);
  });
  it('should return the result of task.taskRun() for sync', () => {
    const task = generateTask({ value: anyObject });

    expect(unsafeRunOk(task)).toEqual(anyObject);
  });
  it('should return the result of task.taskRun() for async', async () => {
    const task = generateTask({ value: anyObject, delayMs: 1 });

    await expect(unsafeRunOk(task)).resolves.toEqual(anyObject);
  });
  it('should run rejected task', async () => {
    const task = Task(() => Promise.reject(new Error('TestError')));
    await expect(unsafeRunOk(task)).rejects.toEqual(new Error('TestError'));
  });
});