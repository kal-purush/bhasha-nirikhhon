import { useEffect, useState } from 'react';

export function useAsync<F extends (...args: R) => Promise<T>, R extends unknown[], T>(asyncFn: F, ...args: R) {
  const [promiseState, setPromiseState] = useState<PromiseState>(PromiseState.IDLE);
  const [result, setResult] = useState<T>();
  const [error, setError] = useState<Error>();

  useEffect(() => {
    if (promiseState !== PromiseState.IDLE) return;
    setResult(undefined);
    setError(undefined);
    setPromiseState(PromiseState.PENDING);
    asyncFn(...args).then(
      result => (setResult(result), setPromiseState(PromiseState.RESOLVED)),
      error => (
        setError(error || new Error('useAsync hook callback rejected with unknown error')),
        setPromiseState(PromiseState.REJECTED)
      ),
    );
    return () => setPromiseState(PromiseState.IDLE);
  }, args);

  return {
    result,
    error,
    isLoading: promiseState === PromiseState.PENDING,
    isRejected: promiseState === PromiseState.REJECTED,
    isResolved: promiseState === PromiseState.RESOLVED,
  } as UsePromiseReturn<T>;
}

enum PromiseState {
  PENDING,
  REJECTED,
  RESOLVED,
  IDLE,
}

interface IdleState {
  isLoading: false;
  isRejected: false;
  isResolved: false;
  result: undefined;
  error: undefined;
}

interface LoadingState {
  isLoading: true;
  isRejected: false;
  isResolved: false;
  result: undefined;
  error: undefined;
}

interface RejectedState {
  isLoading: false;
  isRejected: true;
  isResolved: false;
  result: undefined;
  error: Error;
}

interface ResolvedState<T> {
  isLoading: false;
  isRejected: false;
  isResolved: true;
  result: T;
  error: undefined;
}

type UsePromiseReturn<T> = IdleState | LoadingState | RejectedState | ResolvedState<T>;