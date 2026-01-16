import React from 'react';

type Callback = (...args: unknown[]) => void | Promise<void>;

export function useDebounce<T extends Callback>(callback: T, delay: number): [T, () => void] {
  const [timeoutId, setTimeoutId] = React.useState<NodeJS.Timeout | null>(null);

  const debouncedCallback = React.useCallback(
    (...args: Parameters<T>) => {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }

      const newTimeoutId = setTimeout(() => {
        callback(...args);
        setTimeoutId(null);
      }, delay);

      setTimeoutId(newTimeoutId);
    },
    [callback, delay, timeoutId],
  ) as T;

  const cancelDebounce = React.useCallback(() => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      setTimeoutId(null);
    }
  }, [timeoutId]);

  return [debouncedCallback, cancelDebounce];
}