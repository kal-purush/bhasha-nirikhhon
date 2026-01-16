import { Action } from '@ngrx/store';
import {ITransaction} from '../../models/Transaction.model';

/*
  TRANSACTION ACTIONS
*/

export enum TransactionActionTypes {
  TRANSACTION_LOAD = '[Dashboard Page] Transaction Load',
  TRANSACTION_FAIL = '[Dashboard Page] Transaction Fail',
  TRANSACTION_SUCCESS = '[Dashboard Page] Transaction Success',
  TRANSACTION_ADD = '[Dashboard Page] Add Transaction Item',
  TRANSACTION_ADD_FAIL = '[Dashboard Page] Add Transaction Item Fail',
  TRANSACTION_ADD_SUCCESS = '[Dashboard Page] Add Transaction Item Success',
  TRANSACTION_UPDATE = '[Dashboard Page] Update Transaction Item',
  TRANSACTION_UPDATE_FAIL = '[Dashboard Page] Update Transaction Item Fail',
  TRANSACTION_UPDATE_SUCCESS = '[Dashboard Page] Update Transaction Item Success',
  TRANSACTION_DELETE = '[Dashboard Page] Delete Transaction Item',
  TRANSACTION_DELETE_FAIL = '[Dashboard Page] Delete Transaction Item Fail',
  TRANSACTION_DELETE_SUCCESS = '[Dashboard Page] Delete Transaction Item Success'
}


export class LoadTransactions implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_LOAD;
}

export class LoadTransactionsFail implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_FAIL;
  constructor(public payload: any) {}
}

export class LoadTransactionsSuccess implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_SUCCESS;
  constructor(public payload: ITransaction[]) {}
}

export class AddTransaction implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_ADD;
  constructor(public payload: ITransaction) {}
}

export class AddTransactionFail implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_ADD_FAIL;
}

export class AddTransactionSuccess implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_ADD_SUCCESS;
  constructor(public payload: ITransaction) {}
}


export class UpdateTransaction implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_UPDATE;
  constructor(public payload: any) {}
}

export class UpdateTransactionFail implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_UPDATE_FAIL;
}

export class UpdateTransactionSuccess implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_UPDATE_SUCCESS;
  constructor(public payload: any) {}
}

export class DeleteTransaction implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_DELETE;
}

export class DeleteTransactionFail implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_DELETE_FAIL;
}

export class DeleteTransactionSuccess implements Action {
  readonly type = TransactionActionTypes.TRANSACTION_DELETE_SUCCESS;
  constructor(public payload: any) {}
}

export type TransactionActions =
  | LoadTransactions
  | LoadTransactionsFail
  | LoadTransactionsSuccess
  | AddTransaction
  | AddTransactionFail
  | AddTransactionSuccess
  | UpdateTransaction
  | UpdateTransactionFail
  | UpdateTransactionSuccess
  | DeleteTransaction
  | DeleteTransactionFail
  | DeleteTransactionSuccess;