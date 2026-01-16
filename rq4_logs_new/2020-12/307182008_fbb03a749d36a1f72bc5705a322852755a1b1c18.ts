import React from 'react';
import { useDispatch } from 'react-redux';

import { Either } from 'helpers/either';
import { actions as isLoadingActions } from 'store/reducers/isLoading';

export interface ILoadDataProps<E, T> {
  task: Promise<Either<E, T>>;
  onError: (error: E) => Promise<void> | void;
}

export interface ILoadedData<T> {
  data: T | undefined;
  refetch: () => void;
}

export const useLoadData = <E, T>({ task, onError }: ILoadDataProps<E, T>): ILoadedData<T> => {
  const [refetchCount, setRefetchCount] = React.useState(0);
  const [data, setData] = React.useState<T>();
  const dispatch = useDispatch();

  React.useEffect(() => {
    (async () => {
      dispatch(isLoadingActions.setIsLoading(true));
      const res = await task;
      dispatch(isLoadingActions.setIsLoading(false));

      res
        .rightSideEffect(newData => setData(newData))
        .leftSideEffect(err => onError(err));
    })();
    // eslint-disable-next-line
  }, [refetchCount]);

  return {
    data,
    refetch: () => setRefetchCount(prevCount => prevCount + 1)
  };
};