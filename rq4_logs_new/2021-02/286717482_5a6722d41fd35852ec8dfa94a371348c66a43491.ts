import { createSlice, PayloadAction } from '@reduxjs/toolkit';

import { IExpense } from '../../../components/expenses/IExpense';
import { create } from '../../../api';
import { isAuthenticated } from '../../../api/auth';
import { AppThunk } from '../../store';

type OnchangePayloadType = {
    name: string,
    value: any,
}

type NewExpenseState = {
    isExpenseSuccessfullyCreated: boolean,
    isLoading: boolean,
    serverError: string | null,
} & IExpense

const initialCreateExpenseState: NewExpenseState = {
  title: '',
  amount: 0,
  category: {
    _id: '',
    title: '',
  },
  notes: '',
  incurredOn: Date.now(),
  isExpenseSuccessfullyCreated: false,
  isLoading: false,
  serverError: null,
};

export function createExpense(expenseData: IExpense, cb: Function): AppThunk {
  return async (dispatch) => {
    const { token } = isAuthenticated();
    let data: any;
    try {
      dispatch(setLoading(true));
      data = await create('expenses', expenseData, token);
    } catch (error) {
      dispatch(setLoading(false));
      dispatch(setServerError(error.toString()));

      return;
    }

    dispatch(setLoading(false));
    if (data.success) {
      dispatch(reset(initialCreateExpenseState));
      dispatch(createExpenseSuccessful(true));
      cb();
    } else {
      // in case of bad request errors
      dispatch(setServerError(data.message));
    }
  };
}

const createExpenseSlice = createSlice({
  name: 'createExpense',
  initialState: initialCreateExpenseState,
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
    createExpenseSuccessful(state, action: PayloadAction<boolean>) {
      state.isExpenseSuccessfullyCreated = action.payload;
    },
    reset(state, action: PayloadAction<NewExpenseState>) {
      return action.payload;
    },
  },
});

export const {
  onValueChange,
  setLoading,
  setServerError,
  createExpenseSuccessful,
  reset,
} = createExpenseSlice.actions;

export const createExpenseReducer = createExpenseSlice.reducer;