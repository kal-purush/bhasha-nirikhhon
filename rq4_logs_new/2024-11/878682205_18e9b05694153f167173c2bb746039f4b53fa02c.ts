// src/useCounter.ts
import { useState, useCallback } from 'react';

export interface UseCounterOptions {
  initial?: number;
  min?: number;
  max?: number;
}

const useCounter = (options: UseCounterOptions = {}) => {
  const { initial = 0, min = -Infinity, max = Infinity } = options;
  const [count, setCount] = useState(initial);

  const increment = useCallback(() => {
    setCount((prev) => Math.min(prev + 1, max));
  }, [max]);

  const decrement = useCallback(() => {
    setCount((prev) => Math.max(prev - 1, min));
  }, [min]);

  return { count, increment, decrement };
};

export default useCounter;