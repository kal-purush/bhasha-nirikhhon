import {
  WS_FEED_ORDERS_CONNECT,
  WS_FEED_ORDERS_ERROR,
  WS_FEED_ORDERS_DISCONNECT,
  WS_FEED_RECEIVED_MESSAGE,
} from '../constants/feed';
import { TFeedOrder } from "../reducers/feed";

export interface IWsAction {
  readonly type: string,
  payload?: any
}

export interface IWsFeedOrdersConnect extends IWsAction {
  readonly type: typeof WS_FEED_ORDERS_CONNECT;
}
export interface IWsFeedReceivedMessage extends IWsAction {
  readonly type: typeof WS_FEED_RECEIVED_MESSAGE;
  orders: Array<TFeedOrder>,
  total: number,
  totalToday: number
}
export interface IWsFeedOrdersError extends IWsAction {
  readonly type: typeof WS_FEED_ORDERS_ERROR;
}
export interface IWsFeedOrdersDisconnect extends IWsAction {
  readonly type: typeof WS_FEED_ORDERS_DISCONNECT;
}

export type TFeedActions = 
  IWsFeedOrdersConnect |
  IWsFeedReceivedMessage |
  IWsFeedOrdersError |
  IWsFeedOrdersDisconnect;