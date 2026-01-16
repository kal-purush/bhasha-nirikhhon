import { createSelector, MemoizedSelector } from '@ngrx/store';
import {TransactionsSelectors} from '../../modules/dashboard/store';

export const selectError: MemoizedSelector<object, string> = createSelector(
  TransactionsSelectors.selectTransactionsError,
  (transactionsError: string) => {
    return transactionsError;
  }
);

export const selectIsLoading: MemoizedSelector<
  object,
  boolean
  > = createSelector(
  TransactionsSelectors.selectTransactionsIsLoading,
  (transactions: boolean) => {
    return transactions;
  }
);