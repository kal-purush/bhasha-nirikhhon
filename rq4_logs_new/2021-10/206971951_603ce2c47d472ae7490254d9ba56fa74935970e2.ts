import { renderHook } from '@testing-library/react-hooks';
import useId from '../useId';

describe('useId general behavior', () => {
  test('generates id with "kitchen" as default prefix', () => {
    const { result } = renderHook(() => useId());
    expect(result.current).toMatch(/kitchen/);
  });

  test('uses a dash after prefix', () => {
    const { result } = renderHook(() => useId());
    expect(result.current).toMatch(/kitchen-/);
  });

  test('appends a number after the prefix', () => {
    const { result } = renderHook(() => useId());
    expect(result.current).toMatch(/kitchen-\d/);
  });

  test('uses custom prefix when supplied', () => {
    const { result } = renderHook(() => useId('test-prefix'));
    expect(result.current).toMatch(/test-prefix-\d+/);
  });

  test('keeps the same id between rerenders', () => {
    const { result, rerender } = renderHook(() => useId('test-prefix'));

    const firstRender = result.current[0];
    rerender();
    const secondRender = result.current[0];
    expect(firstRender).toBe(secondRender);
  });

  test('generate a new id when prefix is changed', () => {
    const { result, rerender } = renderHook(
      (initial: string) => useId(initial),
      { initialProps: 'test-one' }
    );
    expect(result.current).toMatch(/test-one-\d+/);

    rerender('test-two');
    expect(result.current).toMatch(/test-two-\d+/);
  });

  test('generated id increases with use of same prefix', () => {
    const { result: resultOne } = renderHook(() => useId());
    const { result: resultTwo } = renderHook(() => useId());
    const { result: resultThree } = renderHook(() => useId());

    const ids = [resultOne.current, resultTwo.current, resultThree.current]
      .map((value) => value.match(/\d+/)?.pop())
      .filter(<T>(value: T | undefined): value is T => value !== undefined)
      .map((value) => parseInt(value));

    expect(ids).toHaveLength(3);

    expect(ids[0]).toBeLessThan(ids[1]);
    expect(ids[1]).toBeLessThan(ids[2]);
  });
});