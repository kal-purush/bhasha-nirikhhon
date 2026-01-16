import { createSlice, PayloadAction } from '@reduxjs/toolkit';

import { IExpense } from '../../../components/expenses/IExpense';
import { update } from '../../../api';
import { isAuthenticated } from '../../../api/auth';
import { AppThunk } from '../../store';

type OnchangePayloadType = {
    name: string,
    value: any,
}

type EditExpenseState = {
    isLoading: boolean,
    serverError: string | null,
} & IExpense

const initialEditExpenseState: EditExpenseState = {
  _id: '',
  title: '',
  amount: 0,
  category: {
    _id: '',
    title: '',
  },
  notes: '',
  incurredOn: Date.now(),
  isLoading: false,
  serverError: null,
};

export function editExpense(expenseId: string, expenseData: IExpense, cb: Function): AppThunk {
  return async (dispatch) => {
    const { token } = isAuthenticated();
    let data: any;
    try {
      dispatch(setLoading(true));
      data = await update('expenses', expenseId, expenseData, token);
    } catch (error) {
      dispatch(setLoading(false));
      dispatch(setServerError(error.toString()));

      return;
    }

    dispatch(setLoading(false));
    if (data.success) {
      dispatch(reset(initialEditExpenseState));
      cb();
    } else {
      // in case of bad request errors
      dispatch(setServerError(data.message));
    }
  };
}

const editExpenseSlice = createSlice({
  name: 'editExpense',
  initialState: initialEditExpenseState,
  reducers: {
    onValueChange(state, action: PayloadAction<OnchangePayloadType>) {
      const { name, value } = action.payload;
      state[name] = value;
    },
    setLoading(state, action: PayloadAction<boolean>) {
      state.isLoading = action.payload;
    },
    setServerError(state, action: PayloadAction<string | null>) {
      state.serverError = action.payload;
    },
    resetStateToSelectedExpenseValues(state, action: PayloadAction<EditExpenseState>) {
      return action.payload;
    },
    reset(state, action: PayloadAction<EditExpenseState>) {
      return action.payload;
    },
  },
});

export const {
  onValueChange,
  setLoading,
  setServerError,
  reset,
  resetStateToSelectedExpenseValues,
} = editExpenseSlice.actions;

export const editExpenseReducer = editExpenseSlice.reducer;