import { Result, unsafeRun } from '@w5s/core';
import { describe, expect, it, vi } from 'vitest';
import { randomBoolean } from './randomBoolean.js';
import { RandomGenerator } from './randomGenerator.js';
import { RandomValue } from './randomValue.js';

describe('randomBoolean', () => {
  const generatorOf = (value: number) => RandomGenerator(() => RandomValue(value));

  it('should use defaultGenerator', async () => {
    const nextRandom = 0.123;

    vi.spyOn(RandomGenerator.defaultRef.current, 'taskRun').mockImplementation((resolve) =>
      resolve(RandomValue(nextRandom))
    );
    expect(unsafeRun(randomBoolean())).toEqual(Result.Ok(false));
  });
  it.each([
    [{ genValue: 0, trueWeight: 0.5 }, false],
    [{ genValue: 0.5, trueWeight: 0.5 }, false],
    [{ genValue: 0.5 + Number.EPSILON, trueWeight: 0.5 }, true],
    [{ genValue: 1, trueWeight: 0.5 }, true],
    [{ genValue: 0.5 + Number.EPSILON, trueWeight: 0.5 + Number.EPSILON }, false],
  ])('should return correct bounded values %s', async ({ genValue, trueWeight }, expected) => {
    const gen = generatorOf(genValue);
    const genBool = randomBoolean(trueWeight, gen);
    expect(unsafeRun(genBool)).toEqual(Result.Ok(expected));
  });
});