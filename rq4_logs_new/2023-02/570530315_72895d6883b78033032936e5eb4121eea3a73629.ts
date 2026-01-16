import { PayloadAction } from '@reduxjs/toolkit';
import { TFeedOrder } from '../../utils/types';

import { 
  WS_PROFILE_FEED_CONNECTION_START,
  WS_PROFILE_FEED_CONNECTION_SUCCESS,
  WS_PROFILE_FEED_CONNECTION_ERROR,
  WS_PROFILE_FEED_GET_MESSAGE,
  WS_PROFILE_FEED_SEND_MESSAGE,
  WS_PROFILE_FEED_CONNECTION_CLOSE,
  WS_PROFILE_FEED_CONNECTION_CLOSED,
} from '../constants/feed';

export const wsProfileFeedActions = {
  wsInit: WS_PROFILE_FEED_CONNECTION_START,
  onOpen: WS_PROFILE_FEED_CONNECTION_SUCCESS,
  onError: WS_PROFILE_FEED_CONNECTION_ERROR,
  onMessage: WS_PROFILE_FEED_GET_MESSAGE,
  wsSendMessage: WS_PROFILE_FEED_SEND_MESSAGE,
  wsClose: WS_PROFILE_FEED_CONNECTION_CLOSE,
  onClose: WS_PROFILE_FEED_CONNECTION_CLOSED,
};

export interface IWsAction {
  readonly type: string;
  payload?: any;
}

export interface IWsProfileFeedConnectionStart extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_CONNECTION_START
}
export interface IWsProfileFeedConnectionSuccess extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_CONNECTION_SUCCESS,
  payload: PayloadAction
}
export interface IWsProfileFeedConnectionError extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_CONNECTION_ERROR,
  payload: PayloadAction
}
export interface IWsProfileFeedGetMessage extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_GET_MESSAGE,
  orders: Array<TFeedOrder>,
  total: number,
  totalToday: number
}
export interface IWsProfileFeedSendMessage extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_SEND_MESSAGE
}
export interface IWsProfileFeedConnectionClose extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_CONNECTION_CLOSE
}
export interface IWsProfileFeedConnectionClosed extends IWsAction {
  readonly type: typeof WS_PROFILE_FEED_CONNECTION_CLOSED,
  payload: PayloadAction
}

export type TProfileFeedActions = 
IWsProfileFeedConnectionStart |
IWsProfileFeedConnectionSuccess |
IWsProfileFeedConnectionError |
IWsProfileFeedGetMessage |
IWsProfileFeedSendMessage |
IWsProfileFeedConnectionClose |
IWsProfileFeedConnectionClosed;