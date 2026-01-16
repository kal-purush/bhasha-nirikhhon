import { renderHook, act } from '@testing-library/react-hooks';
import useBoolean from '../useBoolean';

describe('useBoolean general behaviour', () => {
  test('iniates with "false" by default', () => {
    const { result } = renderHook(() => useBoolean());
    const [value] = result.current;
    expect(value).toBe(false);
  });

  test('initial value sets initial state', () => {
    const { result } = renderHook(() => useBoolean(true));
    const [value] = result.current;
    expect(value).toBe(true);
  });

  test('setter updates state', () => {
    const { result } = renderHook(() => useBoolean(false));

    expect(result.current[0]).toBe(false);
    act(() => result.current[1](true));
    expect(result.current[0]).toBe(true);
    act(() => result.current[1](false));
    expect(result.current[0]).toBe(false);
  });
});