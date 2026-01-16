import { renderHook } from '@testing-library/react-hooks';
import { useDestroy } from '../useDestroy';

describe('useDestroy general behavior', () => {
  test('does not invoked callback on mount', () => {
    const mock = jest.fn();
    renderHook(() => useDestroy(mock));
    expect(mock).not.toBeCalled();
  });

  test('does not invoked callback between re-renders', () => {
    const mock = jest.fn();
    const { rerender } = renderHook(() => useDestroy(mock));

    expect(mock).not.toBeCalled();
    rerender();
    expect(mock).not.toBeCalled();
  });

  test('invokes callback on unmount', () => {
    const mock = jest.fn();
    const { unmount } = renderHook(() => useDestroy(mock));

    expect(mock).not.toBeCalled();
    unmount();
    expect(mock).toBeCalledTimes(1);
  });
});