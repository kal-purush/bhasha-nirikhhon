import * as React from 'react';

import { AsyncAction, AsyncState, UseAsyncReturn } from './interface';

function useSafeDispatch<Action>(dispatch: React.Dispatch<Action>) {
  const mounted = React.useRef(false);

  React.useLayoutEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  return React.useCallback(
    (...args: Parameters<React.Dispatch<Action>>) => {
      if (mounted.current) {
        dispatch(...args);
      }
    },
    [dispatch],
  );
}

function asyncReducer<DataType>(state: AsyncState<DataType>, action: AsyncAction<DataType>) {
  switch (action.type) {
    case 'pending': {
      return { status: 'pending' as const, data: null, error: null };
    }
    case 'resolved': {
      return { status: 'resolved' as const, data: action.data, error: null };
    }
    case 'rejected': {
      return { status: 'rejected' as const, data: null, error: action.error };
    }
    default: {
      throw new Error(`Unhandled action: ${JSON.stringify(action)}`);
    }
  }
}

function useAsync<DataType>(initialState?: AsyncState<DataType>): UseAsyncReturn<DataType> {
  const [state, unsafeDispatch] = React.useReducer<
    React.Reducer<AsyncState<DataType>, AsyncAction<DataType>>
  >(asyncReducer, {
    status: 'idle',
    data: null,
    error: null,
    ...initialState,
  });

  const dispatch = useSafeDispatch(unsafeDispatch);

  const { data, error, status } = state;

  const run = React.useCallback(
    (promise: Promise<DataType>) => {
      dispatch({ type: 'pending' });
      promise.then(
        (d: DataType) => {
          dispatch({ type: 'resolved', data: d });
        },
        (e: Error) => {
          dispatch({ type: 'rejected', error: e });
        },
      );
    },
    [dispatch],
  );

  return {
    error,
    status,
    data,
    run,
    isIdle: status === 'idle',
    isLoading: status === 'pending',
    isLoadingOrIdle: status === 'pending' || status === 'idle',
    isSuccess: status === 'resolved',
    isError: status === 'rejected',
  };
}

export { useAsync };