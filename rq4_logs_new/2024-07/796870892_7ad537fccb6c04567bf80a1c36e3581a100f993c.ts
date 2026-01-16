import { useCallback, useRef } from "react";
import { useSetQueryParam } from "./useSetQueryParam";

// Custom hook for debouncing a function
export function useDebouncedSetQueryParam(delay: number = 150) {
  const { setQueryParam } = useSetQueryParam();
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const debouncedSetQueryParam = useCallback(
    (newParams: Record<string, string>) => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
      timeoutRef.current = setTimeout(() => {
        Object.keys(newParams).forEach((key) => setQueryParam(key, newParams[key]));
      }, delay);
    },
    [setQueryParam, delay]
  );

  return debouncedSetQueryParam;
}