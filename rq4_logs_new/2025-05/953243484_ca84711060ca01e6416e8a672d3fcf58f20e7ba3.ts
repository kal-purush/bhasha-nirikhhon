/* eslint-disable @typescript-eslint/no-explicit-any */
import { useCallback, useRef } from "react";

function useThrottle<T extends (...args: any[]) => void>(
  fn: T,
  delay: number,
): T {
  const lastCall = useRef<number>(0);

  const throttledFn = useCallback(
    (...args: any[]) => {
      const now = Date.now();
      if (now - lastCall.current >= delay) {
        lastCall.current = now;
        fn(...args);
      }
    },
    [fn, delay],
  );

  return throttledFn as T;
}

export default useThrottle;