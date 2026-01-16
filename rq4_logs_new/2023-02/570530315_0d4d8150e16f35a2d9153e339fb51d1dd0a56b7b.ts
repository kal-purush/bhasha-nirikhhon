import { IMessage } from '../../utils/types';
import {
  WS_CONNECTION_START,
  WS_CONNECTION_SUCCESS,
  WS_CONNECTION_ERROR,
  WS_CONNECTION_CLOSED,
  WS_GET_MESSAGE,
  WS_SEND_MESSAGE,
  WS_USER_NAME_UPDATE
} from '../constants/websocket';

export interface IWsConnectionStart {
  readonly type: typeof WS_CONNECTION_START
  payload?: string
}
export interface IWsConnectionSuccess {
  readonly type: typeof WS_CONNECTION_SUCCESS
  payload?: Event
}
export interface IWsConnectionError {
  readonly type: typeof WS_CONNECTION_ERROR
  payload?: Event
}
export interface IWsConnectionClosed {
  readonly type: typeof WS_CONNECTION_CLOSED
  payload?: Event
}
export interface IWsGetMessage {
  readonly type: typeof WS_GET_MESSAGE
  payload: IMessage
}
export interface IWsSendMessage {
  readonly type: typeof WS_SEND_MESSAGE
  payload: IMessage
}
export interface IWsUserNameUpdate {
  readonly type: typeof WS_USER_NAME_UPDATE
  payload: string
}

export type TWsActions = 
  IWsConnectionStart |
  IWsConnectionSuccess |
  IWsConnectionError |
  IWsConnectionClosed |
  IWsGetMessage |
  IWsSendMessage |
  IWsUserNameUpdate;

export const wsConnectionSuccess = (): IWsConnectionSuccess => {
  return {
    type: WS_CONNECTION_SUCCESS,
  };
};

export const wsConnectionError = (): IWsConnectionError => {
  return {
    type: WS_CONNECTION_ERROR
  };
};

export const wsConnectionClosed = (): IWsConnectionClosed => {
  return {
    type: WS_CONNECTION_CLOSED
  };
};

export const wsGetMessage = (message: IMessage): IWsGetMessage => {
  return {
    type: WS_GET_MESSAGE,
    payload: message
  };
};

export const wsSendMessage = (message: IMessage): IWsSendMessage => {
  return {
    type: WS_SEND_MESSAGE,
    payload: message
  };
};

export const wsUserNameUpdate = (userName: string): IWsUserNameUpdate => {
  return {
    type: WS_USER_NAME_UPDATE,
    payload: userName
  };
};