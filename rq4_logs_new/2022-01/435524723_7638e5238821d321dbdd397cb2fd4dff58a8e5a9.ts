import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import { TradeInventoryWrite, TradeSelectedItems, TradeSelectedSurvivor, TradeWrite } from '../../models/trade';

import { RootState } from '../store';

interface TradeState {
  sender?: TradeInventoryWrite;
  receiver?: TradeInventoryWrite;
}

const initialState: TradeState = {
  sender: undefined,
  receiver: undefined,
}

export const survivorSlice = createSlice({
  name: 'trade',
  initialState,
  reducers: {
    setSelectedSurvivor: (state: TradeState, action: PayloadAction<TradeSelectedSurvivor | undefined>) => {
      if (action.payload !== undefined) {
        state.sender = { survivor_id: action.payload.senderId, items: [] };
        state.receiver = { survivor_id: action.payload.receiverId, items: [] };
      } else {
        state.sender = undefined;
        state.receiver = undefined;
      }
    },

    setSelectedItems: (state: TradeState, action: PayloadAction<TradeSelectedItems>) => {
      state.sender = { ...state.sender, items: action.payload.senderItems };
      state.receiver = { ...state.receiver, items: action.payload.receiverItems };
    },
  },
});

export const { setSelectedSurvivor, setSelectedItems } = survivorSlice.actions;

export const selectTradeSurvivors = (state: RootState): TradeSelectedSurvivor => ({
  senderId: state.trade.sender?.survivor_id,
  receiverId: state.trade.receiver?.survivor_id,
});

export const selectTradeWrite = (state: RootState): TradeWrite => ({
  sender: {
    ...state.trade.sender,
    items: (state.trade.sender?.items || []).filter(item => item.quantity > 0),
  },
  receiver: {
    ...state.trade.receiver,
    items: (state.trade.receiver?.items || []).filter(item => item.quantity > 0),
  },
});

export default survivorSlice.reducer;