import {TransactionActions, TransactionActionTypes} from '../actions';
import {ITransaction} from '../../models/Transaction.model';

export interface TransactionsState {
  transactions: ITransaction[];
  loading: boolean;
  loaded: boolean;
}

export const initialState: TransactionsState = {
  transactions: [],
  loading: false,
  loaded: false
};

export function reducers(state = initialState, action: TransactionActions): TransactionsState {
  switch (action.type) {

    case TransactionActionTypes.TRANSACTION_LOAD: {
      return {
        ...state,
        loading: true
      };
    }
    case TransactionActionTypes.TRANSACTION_LOAD_FAIL: {
      return {
        ...state,
        loading: false,
        loaded: false
      };
    }
    case TransactionActionTypes.TRANSACTION_LOAD_SUCCESS: {
      const transactions = [...state.transactions, ...action.payload];
      return {
        ...state,
        transactions,
        loading: false,
        loaded: true
      };
    }

    // TODO ADD CASES
    case TransactionActionTypes.TRANSACTION_ADD: {
      return {
        ...state,
        loading: true,
        loaded: false
      };
    }
    case TransactionActionTypes.TRANSACTION_ADD_FAIL: {
      return {
        ...state,
        loading: false,
        loaded: false
      };
    }
    case TransactionActionTypes.TRANSACTION_ADD_SUCCESS: {
      const transactions = [...state.transactions, action.payload];
      return {
        ...state,
        transactions,
        loading: false,
        loaded: true
      };
    }

    // TO DO UPDATE CASES
    case TransactionActionTypes.TRANSACTION_UPDATE: {
      return {
        ...state,
        loading: true,
        loaded: false
      };
    }
    case TransactionActionTypes.TRANSACTION_UPDATE_FAIL: {
      return state;
    }

    // TODO deal with update
    // case TransactionActionTypes.TRANSACTION_UPDATE_SUCCESS: {
    //   const transaction = state.transactions.map(_transaction => {
    //     if (_transaction.id === action.payload.id) {
    //       return action.payload;
    //     } else {
    //       return transaction;
    //     }
    //   });
    //   return {
    //     ...state,
    //     transaction,
    //     loaded: true,
    //     loading: false
    //   };
    // }
    //   TODO deal with removal
    //   // TO DO REMOVE CASES
    //   case TransactionActionTypes.TRANSACTION_REMOVE_ALL: {
    //     return {
    //       ...state,
    //       loading: true,
    //       loaded: false
    //     };
    //   }
    //   case TransactionActionTypes.TRANSACTION_REMOVE_FAIL: {
    //     return {
    //       ...state,
    //       loading: false,
    //       loaded: false
    //     };
    //   }
    //   case TransactionActionTypes.TRANSACTION_REMOVE_SUCCESS: {
    //     let todo = action.payload;
    //     return {
    //       ...state,
    //       todo,
    //       loading: false,
    //       loaded: true
    //     };
    //   }
    // }
    // default return if case not match
  }
  return state;
}