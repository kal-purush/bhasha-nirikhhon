import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface CoinState {
  amount: number[];
  date: any[];
  coin: any[];
}

const initialState: CoinState = {
  amount: [],
  date: [],
  coin: [],
};

export const CoinInputData = createSlice({
  name: 'coin-amount',
  initialState,
  reducers: {
    saveAmount: (state, action: PayloadAction<any>) => {
      state.amount.push(action.payload);
    },
  },
});

// Action creators are generated for each case reducer function
export const { saveAmount } = CoinInputData.actions;

export default CoinInputData.reducer;